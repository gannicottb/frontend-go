package main

import(
 "database/sql"
 _"github.com/go-sql-driver/mysql"
 "net/http" 
 "fmt"
 "log"
 "time"
 "strconv"
 "strings"
 "io/ioutil" 
)
//Test Query for q2:
//http://ec2-54-85-165-64.compute-1.amazonaws.com:8080/q2?userid=422&tweet_time=2014-02-03%2000:40:09
//http://ec2-54-85-193-234.compute-1.amazonaws.com:8080/tweets/12002667192014-01-22%2012:21:45/about_tweet
var dsn = "cloud9:gradproject@tcp(ec2-54-198-107-252.compute-1.amazonaws.com:3306)/TWEET_DB?parseTime=true"
var hbaseServer = "http://ec2-54-85-193-234.compute-1.amazonaws.com:8080"
var TEAM_ID, AWS_ACCOUNT_ID = "cloud9", "4897-8874-0242"
var db *sql.DB 
const layout = "2006-01-02 15:04:05"
const header = TEAM_ID+","+AWS_ACCOUNT_ID+"\n"
const mysql = 0
const hbase = 1

func q1(w http.ResponseWriter, r *http.Request){	
	timeNow := time.Now().Format(layout)	
  fmt.Fprintf(w, TEAM_ID+","+AWS_ACCOUNT_ID+","+timeNow)
  fmt.Println("Q1 HEARTBEAT at "+timeNow)
}

/*
* Implementation for MySQL backend
*/
func queryMySQL(userId string, tweetTime string) (response string){
	var tweetId uint64		
	//Find tweet_id for given userid and tweettime		
	rows, err := db.Query("SELECT id FROM tweets WHERE userid='"+userId+"' and created_at='"+tweetTime+"' ORDER BY id;")	
	
	if err != nil {
		log.Print(err)	
		response="Error with MySQL Query for"+userId+" and "+tweetTime	
	}else{	
		//Grab the data from the  query
		for rows.Next(){
			err = rows.Scan(&tweetId)
			if err != nil {
				log.Print(err)				
			}else{//no error, convert the tweet_id into a string and concat to resp				
				response += (strconv.FormatUint(tweetId,10)+"\n")
			}
		}
		//Catch lingering errors
		if err := rows.Err(); err != nil {
            log.Print(err)	
    }			
	}	
	return response
}
/*
* Implementation for HBase backend
*/

 func queryHBase(userId string, tweetTime string) (response string){
	//userId = "1200266719"
	//tweetTime= 2014-01-22 12:21:45
	// 
 	//Send GET request to HBase Stargate server
 	res, err := http.Get(hbaseServer+"/tweets/"+userId+tweetTime+",/about_tweet")

	if err != nil {
 		log.Print(err) 
 		response = "Error with HBase GET request for "+userId+" and "+tweetTime
 		return response		
 	}// No error, read the response into tweetIds
 	tweetIds, err := ioutil.ReadAll(res.Body)
 	res.Body.Close()
	if err != nil {
		log.Print(err)
		response = "Error with reading HBase response for "+userId+" and "+tweetTime
		return response
	}// No error, split the tweetIds on ";" and concatenate to response
	results := strings.Split(string(tweetIds), ";")
	for _, id := range results{
		response += (id+"\n")
	}
	return response
}

func q2Query(userId string, tweetTime string, backend int){
	
}


func q2(w http.ResponseWriter, r *http.Request){
	//Extract values from URL
	values := r.URL.Query()
	userId := values["userid"][0]
	tweetTime := values["tweet_time"][0]
	fmt.Println("Q2 REQUEST: with userid="+userId+", tweet_time="+tweetTime)
	
	//Begin response
	response := TEAM_ID+","+AWS_ACCOUNT_ID+"\n"	

	//Query MySQL
	//response += queryMySQL(userId, tweetTime)	
	
	//Query HBase
	//response += queryHBase(userId, tweetTime)

	response += q2Query(userId, tweetTime, mysql)

	//Send response
	fmt.Println("Q2 RESPONSE:"+response)//Print to console what we're returning
	fmt.Fprintf(w, response)
}

func q3(w http.ResponseWriter, r *http.Request){
		userId := r.URL.Query()["userid"][0]

		fmt.Println("Q3 REQUEST: with userid="+userId)

		response := header
		
		response += q3Query(userId, mysql)

		fmt.Fprintf(w, response)
		fmt.Println("Q3 REQUEST:"+response)
}

func main(){
	fmt.Println("Starting up the frontend now...")

	// var err error
	// db, err = sql.Open("mysql", dsn);
	// if err != nil {
	// 	log.Fatal(err)
	// }else{
	// 	fmt.Println("Database open!")
	// }	
  http.HandleFunc("/q1", q1)
	http.HandleFunc("/q2", q2)
	http.HandleFunc("/q3", q3)
  log.Fatal(http.ListenAndServe(":80", nil))
}


