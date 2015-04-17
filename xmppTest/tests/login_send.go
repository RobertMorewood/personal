package main

import (
	"../xmpp"
	"crypto/tls"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
	"runtime"
	"sync"
)

const (
	XMPP_SERVER = "aziz.xmpp.vidao.com"
	USER_PREFIX = "vidao"
	DEFAULT_USER_COUNT = 10000
	START_USER_COUNT = 1
	LOGIN_WORKERS = 20
	LOGIN_RETRY_COUNT = 10
	MESSAGE_DELAY = 50000000 // nanoseconds
	MESSAGE_TIMEOUT = 2000000000 //nanoseconds
)

var userPrefix string = USER_PREFIX
var xmppServer string = XMPP_SERVER
var userCount int = DEFAULT_USER_COUNT
var startUserCount int = START_USER_COUNT
var messageDelay int = MESSAGE_DELAY

type XmppUserType struct {
	Client *xmpp.Client
	Status xmpp.Status
	Time int64
	Retry int
	User int
	BareJid xmpp.JID
}

type MessageStatsType struct {
	messagesReceived int64
	messagesSent int64
	timeouts int64
	averageTransmitTime int64
	longestWait int64
	startTime time.Time
}

// Stress test an XMPP server.
// with predefined users: <USER_PREFIX><COUNT>
// where count starts from "1".
// and password is username.

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Setting parameters
	flag.IntVar(&userCount,"users", DEFAULT_USER_COUNT, "Number of users")
	flag.IntVar(&startUserCount,"start",START_USER_COUNT, "User number at which to start this session.")
	flag.StringVar(&xmppServer,"server", XMPP_SERVER, "Xmpp Server, for example: "+XMPP_SERVER)
	flag.StringVar(&userPrefix,"prefix", USER_PREFIX, 
		"Prefix for usernames, numbers 1,2,3.. will be appended to make usernames")
	var speed int
	flag.IntVar(&speed,"speed",1000000000/MESSAGE_DELAY,"Messages per second to send")
	flag.Parse()
	messageDelay = 1000000000/speed
	
	// Do all logins
	userCountChannel :=loadUserCountChannel(startUserCount, userCount)
	var logins sync.WaitGroup
	logins.Add(LOGIN_WORKERS)
	returnUserChannel := make(chan *XmppUserType, userCount)
	Messages := make(chan xmpp.Incoming)
	loginTimer := time.Now().Unix()
	for worker := 0; worker < LOGIN_WORKERS; worker++ {
		go loginWorker(userCountChannel, worker, Messages, returnUserChannel, &logins) 
	}
	logins.Wait()
	loginTimer = time.Now().Unix() - loginTimer
	close(returnUserChannel)

	xmppUsers := LoadMapPrintStatistics(returnUserChannel, loginTimer)
	if len(xmppUsers)==0 {return}
	
	buffer := make([]byte, 1)
	fmt.Println("\nPaused.  Press the \"Enter\" key to continue.\n(Press enter again to finish.)")
	os.Stdin.Read(buffer)  // pause
	
	working := true
	go func() {
		os.Stdin.Read(buffer) // pause
		working = false
	}()
	
	MessageSent := make(map[string]time.Time, 0)
	var messageStats MessageStatsType	
	clearingLoginMessages := true
	for clearingLoginMessages && working { 
		select {
			case message := <-Messages :
				//fmt.Printf("\nMessage: to %s\n%#v\n",string(message.Client.Jid),message.Stanza)
				ProcessMessage(message.Stanza,MessageSent,&messageStats)
			default:
				clearingLoginMessages = false
		}
	}
	
	//  Message Sending Part
	messageStats.startTime = time.Now()
	nextSend := messageStats.startTime.Add(time.Duration(messageDelay))
	
	for working { 
		// fmt.Println(nextSend)
		select {
		case message := <-Messages :
			//fmt.Printf("\nMessage: to %s\n%#v\n",string(message.Client.Jid),message.Stanza)
			ProcessMessage(message.Stanza,MessageSent,&messageStats)
		default:
			if time.Now().After(nextSend) {
				nextSend = nextSend.Add(time.Duration(MESSAGE_DELAY))
				sendFrom := randXmppUser(xmppUsers)
				if sendFrom==nil { break }
				sendTo := anotherRandXmppUser(xmppUsers,sendFrom.BareJid)
				if sendTo==nil {break}
				//fmt.Printf("%s:%s",sendFrom.Client.Jid.Node(),sendTo.Client.Jid.Node())
				id := strconv.FormatInt(messageStats.messagesSent,10)
				if SendMessage(sendFrom,sendTo,rand_str(5),id) { 
						MessageSent[id] = time.Now()
						messageStats.messagesSent++
						//fmt.Printf("!")
				}
				//fmt.Printf(":\n")
			}
		}
		//fmt.Printf("<")
		runtime.Gosched()
		//fmt.Printf(">")
	}
	fmt.Printf("\n\n\nComplete. Message Send Statistics:\n\n")	
	messageStats.Report()
		
	for _,xmppUser := range xmppUsers {
		xmppUser.Client.Close()
	}
}

func (MessageStats MessageStatsType) Report() {
	fmt.Printf("Sent %d - per second %f\n",MessageStats.messagesSent,
		float32(MessageStats.messagesSent)*1000000000/float32(time.Now().Sub(MessageStats.startTime)))
	fmt.Printf("Back %d (timeouts %d=%.2f%%) - ave transmit time %d ms (longest %d ms)\n",
		MessageStats.messagesReceived, MessageStats.timeouts,
		float32(100*MessageStats.timeouts)/float32(MessageStats.messagesReceived),
		MessageStats.averageTransmitTime/1000000,MessageStats.longestWait/1000000)
}

func ProcessMessage(message xmpp.Stanza, MessageSent map[string]time.Time, MessageStats *MessageStatsType) {
	messageID := message.GetHeader().Id
	if DataIncoming, ok := MessageSent[messageID]; ok {
		ReceivedTime := time.Now()
		difference := int64(ReceivedTime.Sub(DataIncoming))
		if difference > MessageStats.longestWait {
			MessageStats.longestWait = difference
		}
		if difference < MESSAGE_TIMEOUT {
			MessageStats.averageTransmitTime = 
				(MessageStats.averageTransmitTime*MessageStats.messagesReceived+
					difference)/(MessageStats.messagesReceived+1)
			MessageStats.messagesReceived++
		} else {
			MessageStats.timeouts++
		}
		delete(MessageSent,messageID)
		MessageStats.Report()
	}
}

func SendMessage(sendFrom, sendTo *XmppUserType, message, id string) bool {
	stanza := &xmpp.Message{}
	stanza.Header.Id = id
	stanza.Header.From = sendFrom.BareJid
	stanza.Header.To = sendTo.Client.Jid
	stanza.Header.Lang = "en"
	stanza.Body = []xmpp.Text{xmpp.Text{Chardata:message}}
	return SendToClient(sendFrom,stanza)
}
	
func SendToClient(sendFrom *XmppUserType, stanza xmpp.Stanza) bool {
	result := true
	defer func(){ if recover()!=nil {fmt.Printf("!");result=false}}()
	//fmt.Printf("\\")
	sendFrom.Client.Send <- stanza
	//fmt.Printf("/")
	return result
}

func loadUserCountChannel(startUserCount, userCount int) chan int {
	userCountChannel := make(chan int, userCount)
	for count := startUserCount; count < startUserCount+userCount; count++ {
		userCountChannel <- count
	}
	close(userCountChannel)	
	return userCountChannel
}

func loginWorker(in <-chan int, workerNum int, Messages chan xmpp.Incoming, 
				out chan *XmppUserType, logins *sync.WaitGroup) {
	for counter := range in {
		var newXmppUser XmppUserType
		user := strconv.Itoa(counter)
		newXmppUser.User = counter
		password := userPrefix + user
		jid := BareJID(user)
		newXmppUser.BareJid = jid
		done:=false
		// Set up client connection section.
		start:=time.Now()
		for !done && newXmppUser.Retry<LOGIN_RETRY_COUNT {
			status_updater := make(chan xmpp.Status)
			thisLoginDone := make(chan bool)
			go func(worker int, user string, newXmppUser *XmppUserType) {
				for status := range status_updater {
					fmt.Printf("%d) connection status(%s): %s\n", 
						worker, user, xmpp.StatusMessage[status])
					newXmppUser.Status = status
					if status == 5 {
						thisLoginDone <- true
						close(thisLoginDone)
					}
				}
			}(workerNum,user,&newXmppUser)
			tlsConf := tls.Config{InsecureSkipVerify: true}
			client, err := xmpp.NewClient(&jid, password, &tlsConf, nil, 
					xmpp.Presence{}, status_updater, Messages)
			newXmppUser.Time = int64(time.Now().Sub(start))
			newXmppUser.Client = client
			fmt.Printf("%d) NewClient(%s) Status=%d\n", workerNum, user, newXmppUser.Status)
			if err != nil {
				fmt.Printf("%d) NewClientERROR(%d:status=%d): %v\n", workerNum, user, newXmppUser.Status, err)
			} else {
				timeout := make(chan bool)
				go func() {
					time.Sleep(2*time.Second)
					timeout <- true
				}()
				select {
					case <-thisLoginDone :
						out <- &newXmppUser
						done = true
					case <-timeout :
						fmt.Printf("%d) NewClientERROR(%d:status=%d): Timed out waiting for running status.\n", 
							workerNum, user, newXmppUser.Status, err)
				}
			}
			newXmppUser.Retry++
		}
		if !done {
			fmt.Printf("%d) Unable to login user %d after %d tries.\n", workerNum, user, newXmppUser.Retry)
		}
	}
	logins.Done()
	fmt.Printf("Worker %d done.\n",workerNum)
}

func BareJID(user string) xmpp.JID {
	return xmpp.JID(userPrefix + user + "@"+ xmppServer)
}

func randXmppUser(list map[xmpp.JID]*XmppUserType) *XmppUserType {
	for _,result := range list {
		return result
	}
	return nil
}

func anotherRandXmppUser(list map[xmpp.JID]*XmppUserType, element xmpp.JID) *XmppUserType {
	for key,result := range list {
		if key!=element {
			return result
		}
	}
	return nil
}

func rand_str(str_size int) string {
	alphanum := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	bytes:=make([]byte, str_size)
	for count:=0; count<str_size; count++ {
		bytes[count] = alphanum[rand.Intn(len(alphanum))]
	}
	return string(bytes)
}

func LoadMapPrintStatistics(returnUserChannel chan *XmppUserType, loginTimer int64) map[xmpp.JID]*XmppUserType {
	numOfClients := 0
	totalLoginTime := int64(0)
	totalTime := int64(0)
	retries := 0
	retryTotal := 0
	xmppUsers := make(map[xmpp.JID]*XmppUserType)
	for xmpp := range returnUserChannel {
		xmppUsers[xmpp.BareJid] = xmpp
		fmt.Printf("status=%d login time=%d %s\n", 
			xmpp.Status, xmpp.Time, xmpp.Client.Jid);
		if xmpp.Status == 5 {
			numOfClients = numOfClients + 1
			totalLoginTime += xmpp.Time
		}
		if xmpp.Retry>1 { 
			retries++
			retryTotal += xmpp.Retry
		}
		totalTime += xmpp.Time
	}
	fmt.Printf(
		"\n\n\n\nThe clients (Total : %d of %d) which succeed (sending rate - %.2f per second):\n", 
		len(xmppUsers), userCount,
		float32(userCount)/float32(loginTimer))
	if retries>0 {
		fmt.Printf( "%d connections had to retry with an average of %.2f failed logins \n",
			retries,float32(retryTotal)/float32(retries)-1)
	}
	if numOfClients > 0 {
		fmt.Printf(
			"\nSuccessfully logged in users : %d out of %d \n"+
			"Average Succesful NewClient Time: %.2f\n"+
			"Total Average NewClientTime %.2f seconds\n", 
			numOfClients,len(xmppUsers),
			float32(totalLoginTime)/float32(numOfClients)/1000000000, 
			float32(totalTime)/float32(len(xmppUsers))/1000000000)
	} else if len(xmppUsers)>0 {
		fmt.Printf("\nNo clients successfully logged in. Average Time: %d\n",
			totalTime/int64(len(xmppUsers)))
	} else {
		fmt.Printf("\nNo viable clients returned in total time %d\n",totalTime)
	}
	return xmppUsers
}
