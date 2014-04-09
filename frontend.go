package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pmylund/go-cache"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"time"
	"flag"
	"bytes"
)

//Test Query for q2:
//http://ec2-54-85-165-64.compute-1.amazonaws.com:8080/q2?userid=422&tweet_time=2014-02-03%2000:40:09
//http://ec2-54-85-193-234.compute-1.amazonaws.com:8080/tweets/12002667192014-01-22%2012:21:45/about_tweet

//Test for Q3:
//1003121510
//1003274923
//1005208489
//1005468367

var dsnFront = "cloud9:gradproject@tcp("
var dsnBack = ":3306)/TWEET_DB?parseTime=true"
var q2hbaseServer, q3hbaseServer string
var TEAM_ID, AWS_ACCOUNT_ID = "cloud9", "4897-8874-0242"

var shards [10]*sql.DB
var c *cache.Cache

const CACHE_EXPIRATION = 10
const CACHE_PURGE_INTERVAL = 60
const layout = "2006-01-02 15:04:05"

var mysql bool
var debug bool
var caching bool


func q1(w http.ResponseWriter, r *http.Request) {	
	var buffer bytes.Buffer
	buffer.Write([]byte(TEAM_ID + "," + AWS_ACCOUNT_ID+ "," + time.Now().Format(layout)))
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Length", strconv.Itoa(buffer.Len()))	
	buffer.WriteTo(w)
	if debug{fmt.Println("Q1 HEARTBEAT")}
}

func q2(w http.ResponseWriter, r *http.Request) {
	var buffer bytes.Buffer
	var skip = false
	//Extract values from URL
	values := r.URL.Query()
	userId := values["userid"][0]
	tweetTime, err := url.QueryUnescape(values["tweet_time"][0])
	if err != nil {
		log.Print(err)
	}
	if debug{fmt.Println("Q2 REQUEST: with userid=" + userId + ", tweet_time=" + tweetTime)}

	if caching {
	//Check the cache to see if we already have the response
		result, found := c.Get(userId + tweetTime)
		if found { // Cache hit! Use cached value
			buffer.WriteString(result.(string))
			skip = true
		}
	}
	// Cache miss or we're not caching! Query as usual and then cache
	if skip == false {
		buffer.Write([]byte( TEAM_ID + "," + AWS_ACCOUNT_ID + "\n"))
		if mysql {
			q2mysql(userId, tweetTime, &buffer)
		} else {
			q2hbase(userId, tweetTime, &buffer)
		}
		if caching {c.Set(userId+tweetTime, buffer.String(), 0)}	
	}

	//Send response
	w.Header().Set("Content-Type", "text/plain")		
	w.Header().Set("Content-Length", strconv.Itoa(buffer.Len()))
	buffer.WriteTo(w)
	if debug {fmt.Println("Q2 RESPONSE:" + buffer.String())}	
}

func q3(w http.ResponseWriter, r *http.Request) {
	var buffer bytes.Buffer
	var skip = false
	//Extract userId from the request
	userId := r.URL.Query()["userid"][0]
	if debug{fmt.Println("Q3 REQUEST: with userid=" + userId)}
	if caching {
	//Check the cache to see if we already have the response
		result, found := c.Get(userId)
		if found { // Cache hit! Use cached value
			buffer.WriteString(result.(string))			
			skip = true
		}
	}
	// Cache miss or we're not caching! Query as usual and then cache
	if skip == false {
		buffer.Write([]byte( TEAM_ID + "," + AWS_ACCOUNT_ID + "\n"))
		if mysql {
			q3mysql(userId, &buffer)
		} else {
			q3hbase(userId, &buffer)
		}
		if caching {c.Set(userId, buffer.String(), 0)}	
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Length", strconv.Itoa(buffer.Len()))
	buffer.WriteTo(w)
	if debug {fmt.Println("Q3 RESPONSE:" + buffer.String())}
}

/*
* The server attaches handlers and listens for REST requests on port 80
 */
func main() {		
	var err error
	//Grab server addresses from command line args	
	debugPtr := flag.Bool("debug", false, "Turn console output on or off")
	backendPtr := flag.String("b", "default", "either mysql or hbase")	
	cachingPtr := flag.Bool("caching", false, "Turn caching on or off")
    flag.Parse()
    debug = *debugPtr
    caching = *cachingPtr
    if *backendPtr == "mysql"{
        mysql = true       
        for s := range shards{
        	//Create the dsn with the shard IP from the command line args
			dsn := dsnFront + flag.Args()[s] + dsnBack			
			//Open an MySQL connection to the shard
			shards[s], err = sql.Open("mysql", dsn)
			if err != nil {
				log.Fatal(err) //Couldn't open the shard database
			}
			if err = shards[s].Ping(); err != nil {
				log.Fatal(err) //Couldn't ping the shard database
			} else {
				fmt.Println("Shard "+strconv.Itoa(s)+" open!") //Ok, this shard is good to go
			}
		}
    }else if *backendPtr == "hbase"{
        mysql = false        
        //Build the Stargate server addresses from supplied addresses
        q2hbaseServer = "http://" + flag.Args()[0] + ":8080"
		q3hbaseServer = "http://" + flag.Args()[1] + ":8080"
		fmt.Println("Q2 and Q3 hbase servers registered!")
    }else{
        log.Fatal("No backend selected. Run the server with -b=(mysql || hbase)")
    }

	//Use as many cores as Go can find on the machine
	runtime.GOMAXPROCS(runtime.NumCPU())

	//Create a cache with a 10 minute expiration date that purges expired items every 60 seconds
	c = cache.New(CACHE_EXPIRATION*time.Minute, CACHE_PURGE_INTERVAL*time.Second)
	
	//Attach handlers
	http.HandleFunc("/q1", q1)
	http.HandleFunc("/q2", q2)
	http.HandleFunc("/q3", q3)
	fmt.Println("Frontend starting using " + backend() + " for the backend...")
	log.Fatal(http.ListenAndServe(":80", nil))
}

func backend() (str string) {
	if mysql {
		return "MySQL"
	} else {
		return "HBase"
	}
}

/*
* Implementation for Q2 MySQL backend
 */
func q2mysql(userId string, tweetTime string, buffer *bytes.Buffer)  {
	var tweetId uint64
	//Decide which shard to query
	s, err := strconv.ParseUint(userId, 10, 64)
	s = s % 10
	//Find tweet_id for given userid and tweettime
	rows, err := shards[s].Query("SELECT tid FROM tweets WHERE userid='" + userId + "' and created_at='" + tweetTime + "' ORDER BY tid;")

	if err != nil {
		log.Print(err)
		buffer.WriteString(err.Error())
	} else {
		//Grab the data from the  query
		for rows.Next() {
			err = rows.Scan(&tweetId)
			if err != nil {
				buffer.WriteString(err.Error())
			} else { //no error, convert the tweet_id into a string and concat to resp
				buffer.WriteString(strconv.FormatUint(tweetId, 10) + "\n")
			}
		}
		//Catch lingering errors
		if err := rows.Err(); err != nil {
			log.Print(err)
		}
	}	
}

/*
* Implementation for Q2 HBase backend
 */
func q2hbase(userId string, tweetTime string, buffer *bytes.Buffer) {
	//Send GET request to HBase Stargate server
	res, err := http.Get(q2hbaseServer + "/tweets_q2/" + userId + tweetTime + ",/about_tweet")	
	if err != nil {
		log.Print(err)
		buffer.WriteString(err.Error())
		return
	} // No error, read the response into tweetIds
	tweetIds, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Print(err)
		buffer.WriteString(err.Error())
		return 
	} // No error, split the tweetIds on ";" and concatenate to response
	results := strings.Split(string(tweetIds), ";")
	for _, id := range results {
		buffer.WriteString(strings.TrimSpace(id) + "\n")
	}
}

func q3mysql(userId string, buffer *bytes.Buffer) {
	var srcId uint64
	//Decide which shard to query
	s, err := strconv.ParseUint(userId, 10, 64)
	s = s % 10
	rows, err := shards[s].Query("SELECT src_uid FROM retweets WHERE target_uid='" + userId + "';")

	if err != nil {	
		log.Print(err)	
		buffer.WriteString(err.Error())
	} else {
		//Grab the data from the  query
		for rows.Next() {
			err = rows.Scan(&srcId)
			if err != nil {
				log.Print(err)
			} else { //no error, convert the tweet_id into a string and concat to resp
				buffer.WriteString(strconv.FormatUint(srcId, 10) + "\n")
			}
		}
		//Catch lingering errors
		if err := rows.Err(); err != nil {
			log.Print(err)
		}
	}	
}

func q3hbase(userId string, buffer *bytes.Buffer) {
	//Send GET request to HBase Stargate server
	res, err := http.Get(q3hbaseServer + "/tweets_q3/" + userId + ",/about_tweet")

	if err != nil {
		log.Print(err)
		buffer.WriteString(err.Error())
		return
	} // No error, read the response into tweetIds
	userIds, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Print(err)
		buffer.WriteString(err.Error())
		return
	} // No error, split the tweetIds on ";" and concatenate to response
	results := strings.Split(string(userIds), ";")	
	for _, id := range results {
		buffer.WriteString(strings.TrimSpace(id) + "\n")
	}	
}
