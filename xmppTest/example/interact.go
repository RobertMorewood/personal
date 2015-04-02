package main

import (
	"../xmpp"
	"crypto/tls"
	cryptorand "crypto/rand"
	//"encoding/xml"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
//	"strings"
	"time"
	"runtime"
	"sync"
)

type XmppUser struct {
	Client *xmpp.Client
	Status xmpp.Status
	Time int64
	Retry int
}

const (
	XMPP_SERVER = "aziz.xmpp.vidao.com"
	USER_PREFIX = "vidao"
	DEFAULT_USER_COUNT = 100
	LOGIN_WORKERS = 35
	MESSAGE_DELAY = 10000000 // nanoseconds
)

// Stress test an XMPP server.
// with predefined users: <USER_PREFIX><COUNT>
// where count starts from "1".
// and password is username.

func main() {
	runtime.GOMAXPROCS(60)

	fmt.Println("Starting client")
	userCount := flag.Int("numb", DEFAULT_USER_COUNT, "Number of users")
	flag.Parse()
	inChannel := generateXMPPusers(*userCount)

	// Create a return channel for each worker
	var logins sync.WaitGroup
	logins.Add(LOGIN_WORKERS)
	userChannel := make(chan *XmppUser, *userCount)
	Messages := make(chan xmpp.Stanza)
	startRequestTimer := time.Now().Unix()
	for worker := 0; worker < LOGIN_WORKERS; worker++ {
		go loginWorker(inChannel, worker, Messages, userChannel, &logins) 
	}
	logins.Wait()
	endRequestTimer := time.Now().Unix()
	close(userChannel)

	// Statistics follow.
	numOfClients := 0
	totalLoginTime := int64(0)
	totalTime := int64(0)
	retries := 0
	retryTotal := 0
	var xmppUsers []*XmppUser
	for xmpp := range userChannel {
		xmppUsers = append(xmppUsers, xmpp)
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
		Client := xmpp.Client
		defer Client.Close()		
	}
	fmt.Printf(
		"\n\n\n\nThe clients (Total : %d of %d) which succeed (sending rate - %.2f per second):\n", 
		len(xmppUsers), *userCount,
		float32(*userCount)/float32(endRequestTimer - startRequestTimer))
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
		fmt.Printf("\n\n\nNo clients successfully logged in. Average Time: %d\n",
			totalTime/int64(len(xmppUsers)))
	} else {
		fmt.Println("\n\n\nNo viable clients returned in total time %d\n",totalTime)
	}
	buffer := make([]byte, 1)
	fmt.Println("\nPaused.  Press the \"Enter\" key to continue.\n(Press enter again to finish.)")
	os.Stdin.Read(buffer)  // pause
	
	//  Message Sending Part
	MessageSent := make(map[string]time.Time, 0)
	
	messagesReceived := 0
	messagesSent := 0
	timeouts := 0
	averageTransmitTime := int64(0)
	longestWait := int64(0)

	startTime := time.Now()
	nextSend := startTime.Add(time.Duration(MESSAGE_DELAY))

	working := true
	go func() {
		os.Stdin.Read(buffer) // pause
		working = false
	}()

	for working { 
		select {
		case message := <-Messages :
			//fmt.Printf("Incoming parsed (client# %v)XML:\n%#v\n\n", thisClient.Jid, object)
			///===============================================================================================
			messageID := message.GetHeader().Id
			if DataIncoming, ok := MessageSent[messageID]; ok {
				ReceivedTime := time.Now()
				difference := int64(ReceivedTime.Sub(DataIncoming))
				if difference > longestWait {
					longestWait = difference
				}
				if difference < 2000000000 {
					averageTransmitTime = (averageTransmitTime*int64(messagesReceived)+difference)/int64(messagesReceived+1)
					messagesReceived++
				} else {
					timeouts++
				}
				delete(MessageSent,messageID)
				fmt.Printf("Sent %d - per second %f\n",messagesSent,
					float32(messagesSent)*1000000000/float32(ReceivedTime.Sub(startTime)))
				fmt.Printf("Back %d (timeouts %.2f%%) - ave transmit time %d (longest %d)\n",
					messagesReceived,float32(100*timeouts)/float32(messagesSent),
					averageTransmitTime,longestWait)
			}
		default:
			if time.Now().After(nextSend) {
				nextSend = nextSend.Add(time.Duration(MESSAGE_DELAY))
				sIndex := rand.Intn(len(xmppUsers))
				for xmppUsers[sIndex].Status !=5 {
					sIndex = rand.Intn(len(xmppUsers))
				}
				rIndex := rand.Intn(len(xmppUsers))
				for rIndex == sIndex || xmppUsers[rIndex].Status != 5 {
					rIndex = rand.Intn(len(xmppUsers))
				}
				sender := xmppUsers[sIndex].Client.Jid
				receiver := xmppUsers[rIndex].Client.Jid
				
				stanza := &xmpp.Message{}
				stanza.Header.Id = strconv.Itoa(messagesSent)
				stanza.Header.From = sender
				stanza.Header.To = receiver
				stanza.Header.Lang = "en"
				stanza.Body = []xmpp.Text{xmpp.Text{Chardata:rand_str(5)}}

				xmppUsers[sIndex].Client.Send <- stanza
				MessageSent[stanza.GetHeader().Id] = time.Now()
				messagesSent++
			}
		}
	}
	fmt.Printf("\n\n\nComplete. Message Send Statistics:\n\n")
	fmt.Printf("Sent %d - per second %f\n",messagesSent,
		float32(messagesSent)*1000000000/float32(time.Now().Sub(startTime)))
	fmt.Printf("Back %d (timeouts %.2f%%) - ave transmit time %d (longest %d)\n",
		messagesReceived,float32(100*timeouts)/float32(messagesSent),
		averageTransmitTime,longestWait)
}

func generateXMPPusers(numOfUsers int) chan int {
	out := make(chan int, numOfUsers)
	for i := 1; i <= numOfUsers; i++ {
		out <- i
	}
	close(out)	
	return out
}

func loginWorker(in <-chan int, workerNum int, Messages chan xmpp.Stanza, 
				out chan *XmppUser, logins *sync.WaitGroup) {
	for counter := range in {
		var newXmppUser XmppUser
		user := strconv.Itoa(counter)
		password := USER_PREFIX + user
		jidStr := USER_PREFIX + user + "@"+ XMPP_SERVER
		fmt.Printf("Worker%d User : %s  Password : %s\n", workerNum, jidStr, password)
		jid := xmpp.JID(jidStr)
		// xmpp.JID is a string of the form <node>@<domain>/<resource>, with /<resource> optional.
		done:=false
		// Set up client connection section.
		start:=time.Now()
		for !done && newXmppUser.Retry<5 {
			status_updater := make(chan xmpp.Status)
			thisLoginDone := make(chan bool)
			go func(worker int, user string, newXmppUser *XmppUser) {
				for status := range status_updater {
					fmt.Printf("connection status(%s): %s\n", 
						user, xmpp.StatusMessage[status])
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
		if newXmppUser.Retry>=5 {
			fmt.Printf("%d) Unable to login user %d after %d tries.\n", workerNum, user, newXmppUser.Retry)
		}
	}
	logins.Done()
	fmt.Printf("Worker %d done.\n",workerNum)
}

func rand_str(str_size int) string {
	alphanum := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, str_size)
	cryptorand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes)
}
