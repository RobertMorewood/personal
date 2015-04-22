package main

import (
	"../xmpp"
	"crypto/tls"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"
	"runtime"
	//"sync"
)

const (
	XMPP_SERVER = "xmpp.vidao.com"
	SERVER_IP = "104.197.17.128"
	SERVER_PORT = 5222
	USER_PREFIX = "vidao"
	DEFAULT_USER_COUNT = 10000
	START_USER_COUNT = 1
	LOGIN_WORKERS = 10
	LOGIN_RETRY_COUNT = 10
	MESSAGE_DELAY = 10000000 // nanoseconds
	MESSAGE_TIMEOUT = 2000000000 //nanoseconds
	TIME_TO_LOGOUT = 3600. // seconds
	TIME_TO_MESSAGE = 60. // seconds
	TIME_TO_PRESENCE = 300. // seconds
	TIME_FORMAT = "2006-01-02_15:04:05.999999999_-0700_MST"
)

var userPrefix string = USER_PREFIX
var xmppServer string = XMPP_SERVER
var serverIP string = SERVER_IP
var serverPort int = SERVER_PORT
var userCount int = DEFAULT_USER_COUNT
var startUserCount int = START_USER_COUNT
var messageDelay int = MESSAGE_DELAY

type XmppUserType struct {
	Client *xmpp.Client
	Status xmpp.Status
	Time int64
	Retry int
	User int
	Index int
	BareJid xmpp.JID
	AwayStatus bool
}

type PendingUserType struct {
	Index int
	User int
}

type globalStatsType struct {
	messagesReceived int64
	messagesSent int64
	presenceSent int64
	subscriptions int64
	timeouts int64
	averageTransmitTime int64
	longestWait int64
	startTime time.Time
	logins int64
	averageLoginTime int64
	usercount int64
	// roster data?
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
	flag.StringVar(&xmppServer,"server", XMPP_SERVER, "Xmpp Server Virtual Host, for example: "+XMPP_SERVER)
	flag.StringVar(&userPrefix,"prefix", USER_PREFIX, 
		"Prefix for usernames, numbers 1,2,3.. will be appended to make usernames")
	flag.StringVar(&serverIP,"ip", XMPP_SERVER, "Xmpp Server IP1, for example: "+SERVER_IP)
	var speed int
	flag.IntVar(&speed,"speed",1000000000/MESSAGE_DELAY,"Messages per second to send")
	flag.Parse()
	messageDelay = 1000000000/speed

	// Create workers
	xmppUsers := make([]*XmppUserType,userCount)
	pendingUsers := make(chan PendingUserType)
	Messages := make(chan xmpp.Incoming)
	loginStats := make(chan time.Duration)
	var globalStats globalStatsType
	go ProcessLoginStats(loginStats,&globalStats)
	for worker := 0; worker < LOGIN_WORKERS; worker++ {
		go loginWorker(pendingUsers, worker, Messages, &xmppUsers) 
		//TODO pass loginStats to login Workers to pass back stats.
	}
	go ProcessingMessages(Messages, &globalStats)
	go LoadPendingUsers(startUserCount,userCount,pendingUsers)
	
	buffer := make([]byte, 1)
	working := true
	go func() {
		os.Stdin.Read(buffer) // pause
		working = false
	}()
	
	// Main Event loop

	timeToEvent := 1/(1/float32(TIME_TO_LOGOUT)+1/TIME_TO_MESSAGE+1/TIME_TO_PRESENCE)
	messageCutOff := timeToEvent/float32(TIME_TO_MESSAGE)
	presenceCutOff := messageCutOff + timeToEvent/float32(TIME_TO_PRESENCE)
	timeToEvent = 1000000000*timeToEvent/float32(userCount)
	rand.Seed(time.Now().Unix())
	nextEventTime := time.Now().Add(time.Duration(float64(timeToEvent)*math.Log(1/(1-rand.Float64()))))
	
	for working {
		time.Sleep(nextEventTime.Sub(time.Now()))
		nextEventTime = time.Now().Add(time.Duration(float64(timeToEvent)*math.Log(1/(1-rand.Float64()))))
		eventType := rand.Float32()
		switch {
			case eventType < messageCutOff :
				sendFrom := randXmppUser(&xmppUsers)
				if sendFrom==nil { break }
				sendTo := anotherRandXmppUser(&xmppUsers,sendFrom)
				if sendTo==nil {break}
				go SendMessage(sendFrom,sendTo,rand_str(5),&globalStats)
			case eventType < presenceCutOff :
				presence := randXmppUser(&xmppUsers)
				if presence==nil {break}
				go SendPresence(presence,&globalStats)
			default :
				expiring := randXmppUser(&xmppUsers)
				if expiring==nil {break}
				xmppUsers[expiring.Index] = nil
				expiring.Client.Close()
				pendingUsers <- PendingUserType{Index:expiring.Index, User:expiring.User}
		}
		runtime.Gosched()
	}
	close(pendingUsers)	
	for _,xmppUser := range xmppUsers {
		xmppUser.Client.Close()
	}
	fmt.Printf("\n\n\nComplete. Message Send Statistics:\n\n")	
	globalStats.Report()
}

func (MessageStats globalStatsType) Report() {
	fmt.Printf("Sent %d - per second %f\n",MessageStats.messagesSent,
		float32(MessageStats.messagesSent)*1000000000/float32(time.Now().Sub(MessageStats.startTime)))
	fmt.Printf("Back %d (timeouts %d=%.2f%%) - ave transmit time %d ms (longest %d ms)\n",
		MessageStats.messagesReceived, MessageStats.timeouts,
		float32(100*MessageStats.timeouts)/float32(MessageStats.messagesReceived),
		MessageStats.averageTransmitTime/1000000,MessageStats.longestWait/1000000)
}

func ProcessingMessages(messages chan xmpp.Incoming, MessageStats *globalStatsType) {
	for message := range messages {
		header:= message.Stanza.GetHeader()
		if DataIncoming,err := time.Parse(TIME_FORMAT,header.Id); err ==nil {
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
			if MessageStats.messagesReceived % 1000 == 0 {
				MessageStats.Report()
			}
		}		
		switch message.Stanza.(type) {
			case *xmpp.Presence: 
				if header.Type == "subscribe" {
					stanza := &xmpp.Presence{}
					stanza.Header.Id = header.Id
					stanza.Header.From = message.Client.Jid.Bare()
					stanza.Header.To = header.From
					stanza.Header.Type = "subscribed"
					if SendToClient(message.Client,stanza) {
						fmt.Println("\n>>> subscribed sent")
						MessageStats.subscriptions++	
						//MessageStats.Report()
					}
				}
		}

	}
}

func SendPresence(xmppUser *XmppUserType, messageStats *globalStatsType){
	stanza := &xmpp.Presence{}
	stanza.Header.Id = time.Now().Format(TIME_FORMAT)
	//stanza.Header.From = xmppUser.BareJid
	if xmppUser.AwayStatus {
		stanza.Show = &xmpp.Data{Chardata:"away"} 
	} else {
		stanza.Show = &xmpp.Data{Chardata:"chat"} 
	}
	if ! SendToClient(xmppUser.Client,stanza) {	
		fmt.Printf("Fail to change status for %s to %s\n",
			xmppUser.BareJid.Node(), stanza.Show.Chardata)
	} else {
			xmppUser.AwayStatus = !xmppUser.AwayStatus
			messageStats.presenceSent++
	}
}
	
func SendMessage(sendFrom, sendTo *XmppUserType, message string, messageStats *globalStatsType) {
	stanza := &xmpp.Message{}
	stanza.Header.Id = time.Now().Format(TIME_FORMAT)
	stanza.Header.From = sendFrom.BareJid
	stanza.Header.To = sendTo.Client.Jid
	stanza.Header.Lang = "en"
	stanza.Body = []xmpp.Text{xmpp.Text{Chardata:message}}
	if SendToClient(sendFrom.Client,stanza) {
		messageStats.messagesSent++
	}
}
	
func SendToClient(sendFrom *xmpp.Client, stanza xmpp.Stanza) bool {
	result := true
	defer func(){ if recover()!=nil {result=false}}()
	sendFrom.Send <- stanza
	return result
}

func LoadPendingUsers(startUserCount, userCount int, pendingUsers chan PendingUserType) {
	for index := 0 ; index < userCount; index++ {
		pendingUsers <- PendingUserType{Index:index, User:index+startUserCount}
	}
}

func ProcessLoginStats(loginStats <-chan time.Duration , globalStats *globalStatsType) {
	for loginTime := range loginStats {
		globalStats.averageLoginTime = 
			(globalStats.averageLoginTime*globalStats.logins + int64(loginTime)) /
				(globalStats.logins+1)
		globalStats.logins++
	}
}

func loginWorker(pendingUsers chan PendingUserType, workerNum int, 
				Messages chan xmpp.Incoming, xmppUsers *[]*XmppUserType) {
	for nextuser := range pendingUsers {
		var newXmppUser XmppUserType
		newXmppUser.Index = nextuser.Index
		newXmppUser.User = nextuser.User
		newXmppUser.BareJid = BareJID(strconv.Itoa(nextuser.User))
		password := newXmppUser.BareJid.Node()
		jid := newXmppUser.BareJid
		// Set up client connection section.
		status_updater := make(chan xmpp.Status)
		thisLoginDone := make(chan bool)
		go func(user *XmppUserType) {
			for status := range status_updater {
				fmt.Printf("%d) connection status(%s): %s\n", 
					user.Index, string(user.BareJid), xmpp.StatusMessage[status])
				if status == 5 {
					thisLoginDone <- true
					close(thisLoginDone)
				}
				if status == 6 {
					var pendingUser PendingUserType
					pendingUser.Index = user.Index
					pendingUser.User = user.User
					go func(user PendingUserType){
						pendingUsers <- user
					}(pendingUser)
				}
			}
		}(&newXmppUser)
		tlsConf := tls.Config{InsecureSkipVerify: true}
		start:=time.Now()
		client, err := xmpp.NewClientFromHost(&jid, password, &tlsConf, nil, 
						xmpp.Presence{}, status_updater, serverIP, serverPort, Messages)
			newXmppUser.Client = client
		fmt.Printf("%d) NewClient(%s) Status=%d\n", workerNum, jid, newXmppUser.Status)
			if err != nil {
				fmt.Printf("%d) NewClientERROR(%s:status=%d): %v\n", 
					workerNum, jid, newXmppUser.Status, err)
				//pendingUsers <- count
			} else {
				timeout := make(chan bool)
				go func() {
					time.Sleep(2*time.Second)
					timeout <- true
				}()
				select {
					case <-thisLoginDone :
						newXmppUser.Time = int64(time.Now().Sub(start))
						(*xmppUsers)[newXmppUser.Index] = &newXmppUser
						//done = true
					case <-timeout :
						fmt.Printf("%d) NewClientERROR(%s:status=%d): Timed out waiting for running status.\n", 
							workerNum, jid, newXmppUser.Status, err)
						//pendingUsers <- count
				}
			}
		runtime.Gosched()
	}
	fmt.Printf("Worker %d done.\n",workerNum)
}

func BareJID(user string) xmpp.JID {
	return xmpp.JID(userPrefix + user + "@"+ xmppServer)
}

func randXmppUser(list *[]*XmppUserType) *XmppUserType {
	limit := len(*list)
	selected := rand.Intn(limit)
	original := selected
	for (*list)[selected]==nil {
		selected = (selected+1)%limit
		if selected == original {
			return nil
		}
	}
	return (*list)[selected]
}

func anotherRandXmppUser(list *[]*XmppUserType, previous *XmppUserType) *XmppUserType {
	selected := randXmppUser(list)
	if selected == previous {
		return nil
	}
	return selected
}

func rand_str(str_size int) string {
	alphanum := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	bytes:=make([]byte, str_size)
	for count:=0; count<str_size; count++ {
		bytes[count] = alphanum[rand.Intn(len(alphanum))]
	}
	return string(bytes)
}
/*
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
*/
