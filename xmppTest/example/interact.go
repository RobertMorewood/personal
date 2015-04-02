package main

import (
	"../xmpp"
	"crypto/tls"
	cryptorand "crypto/rand"
	"encoding/xml"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
	"runtime"
	"sync"
)

type XmppUser struct {
	Client *xmpp.Client
	Status xmpp.Status
	Time int64
}

// Demonstrate the API, and allow the user to interact with an XMPP
// server via the terminal.
func main() {
	runtime.GOMAXPROCS(60)

	fmt.Println("Starting client")
	numberOfUsers := flag.Int("numb", 42, "Number of user")
	flag.Parse()
	inChannel := generateXMPPusers(*numberOfUsers)

	// Create a return channel for each worker
	workers := 10
	var logins sync.WaitGroup
	logins.Add(workers)
	userChannel := make(chan *XmppUser, *numberOfUsers)
	Messages := make(chan xmpp.Stanza)
	startRequestTimer := time.Now().Unix()
	for worker := 0; worker < workers; worker++ {
		go loginWorker(inChannel, worker, Messages, userChannel, &logins) 
	}
	logins.Wait()
	endRequestTimer := time.Now().Unix()
	close(userChannel)
	var xmppUsers []*XmppUser
	for xmpp := range userChannel {
		fmt.Printf("Adding Client %s\n",xmpp.Client.Jid)
		xmppUsers = append(xmppUsers, xmpp)
	}
	
	// Statistics follow.
	numOfClients := 0
	totalLoginTime := int64(0)
	totalTime := int64(0)
	for counter := 0; counter < len(xmppUsers); counter++ {
		fmt.Printf("status=%d login time=%d %s\n", 
			xmppUsers[counter].Status, xmppUsers[counter].Time, 
			xmppUsers[counter].Client.Jid);
		if xmppUsers[counter].Status == 5 {
			numOfClients = numOfClients + 1
			//fmt.Printf("Jid :%v  and  Status :%v \n", xmppUsers[counter].Client.Jid, xmppUsers[counter].Status)
			totalLoginTime += xmppUsers[counter].Time
		}
		totalTime += xmppUsers[counter].Time
		defer xmppUsers[counter].Client.Close()

	}
	fmt.Printf(
		"\n\n\n\nThe clients (Total : %d of %d) which succeed (sending rate - %f per second):\n", 
		len(xmppUsers), *numberOfUsers,
		float32(*numberOfUsers)/float32(endRequestTimer - startRequestTimer))
	if numOfClients > 0 {
		fmt.Printf(
			"\n\n\nSuccessfully logged in users : %d out of %d \n"+
			"Average Succesful NewClient Time: %f\n"+
			"Total Average NewClientTime %f seconds\n", 
			numOfClients,len(xmppUsers),
			float32(totalLoginTime)/float32(numOfClients)/1000000000, 
			float32(totalTime)/float32(len(xmppUsers))/1000000000)
	} else {
		fmt.Printf("\n\n\n No clients successfully logged in. Average Time: %d\n",
			totalTime/int64(len(xmppUsers)))
	}
	buffer := make([]byte, 1)
	os.Stdin.Read(buffer)  // pause
	
	//  Message Sending Part
	MessageSent := make(map[string]time.Time, 0)
	messagesReceived := 0
	messagesSent := 0
	timeouts := 0
	averageTransmitTime := int64(0)
	longestWait := int64(0)
	startTime := time.Now()
	delay := time.Duration(100000)
	nextSend := startTime.Add(delay)
	for {
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
				fmt.Printf("Back %d (timeouts %f%%) - ave transmit time %d (longest %d)\n",
					messagesReceived,float32(100*timeouts)/float32(messagesSent),
					averageTransmitTime,longestWait)
			}
		default:
			if time.Now().After(nextSend) {
				nextSend = nextSend.Add(delay)
				sIndex := rand.Intn(len(xmppUsers))
				for xmppUsers[sIndex].Status !=5 {
					sIndex = rand.Intn(len(xmppUsers))
				}
				rIndex := rand.Intn(len(xmppUsers))
				for rIndex == sIndex || xmppUsers[rIndex].Status != 5 {
					rIndex = rand.Intn(len(xmppUsers))
				}
				sender := strings.Split(string(xmppUsers[sIndex].Client.Jid), "/")[0]
				receiver := strings.Split(string(xmppUsers[rIndex].Client.Jid), "/")[0]
				
				message := "<message id='" + strconv.Itoa(messagesSent) + "' from='" + sender + "' to='" + receiver + "' xml:lang='en' xmlns='jabber:client'><body>" + rand_str(5) + "</body></message>"

				stanza := &xmpp.Message{}
				xmlDecoder := xml.NewDecoder(strings.NewReader(message))
				err := xmlDecoder.Decode(stanza)
				if err == nil {
					//fmt.Printf("Sending:\n%#v\n\n", stanza)
					xmppUsers[sIndex].Client.Send <- stanza
					MessageSent[stanza.GetHeader().Id] = time.Now()
					messagesSent++
				} else {
					fmt.Printf("Parse error: %v\n\n", err)
				}
			}
	}}

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
		var tempXmppUser XmppUser
		user := strconv.Itoa(counter)
		password := "vidao" + user
		jidStr := "vidao" + user + "@aziz.xmpp.vidao.com"
		fmt.Printf("Worker%d User : %s  Password : %s\n", workerNum, jidStr, password)
		jid := xmpp.JID(jidStr)
		// xmpp.JID is a string of the form <node>@<domain>/<resource>, with /<resource> optional.

		// Set up client connection section.
		start:=time.Now()
		for attempt:=0; attempt<5; attempt++ {
			status_updater := make(chan xmpp.Status)
			go func(worker int, user string, tempXmppUser *XmppUser) {
				for status := range status_updater {
					fmt.Printf("connection status(%s): %s\n", 
						user, xmpp.StatusMessage[status])
					tempXmppUser.Status = status
				}
			}(workerNum,user,&tempXmppUser)
			tlsConf := tls.Config{InsecureSkipVerify: true}
			client, err := xmpp.NewClient(&jid, password, &tlsConf, nil, 
					xmpp.Presence{}, status_updater, Messages)
			tempXmppUser.Time = int64(time.Now().Sub(start))
			tempXmppUser.Client = client
			fmt.Printf("%d) NewClient(%s) Status=%d\n", workerNum, user, tempXmppUser.Status)
			if err != nil {
				fmt.Printf("NewClientERROR(%v): %v\n", jid, err)
			} else {
				out <- &tempXmppUser
				break
			}
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
