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
 "os"
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
var db *sql.DB 
const layout = "2006-01-02 15:04:05"
//WHICH BACKEND AM I USING???
var mysql = false
//???????????????????????????

func q1(w http.ResponseWriter, r *http.Request){	
	timeNow := time.Now().Format(layout)	
  fmt.Fprintf(w, TEAM_ID+","+AWS_ACCOUNT_ID+","+timeNow)
  fmt.Println("Q1 HEARTBEAT at "+timeNow)
}

func q2(w http.ResponseWriter, r *http.Request){
	//Extract values from URL
	values := r.URL.Query()
	userId := values["userid"][0]
	tweetTime := values["tweet_time"][0]
	fmt.Println("Q2 REQUEST: with userid="+userId+", tweet_time="+tweetTime)	
	//Begin response
	response := TEAM_ID+","+AWS_ACCOUNT_ID+"\n"	
	if mysql {
		response += q2mysql(userId, tweetTime)
	}else{
		response += q2hbase(userId, tweetTime)
	}	
	//Send response
	fmt.Fprintf(w, response)
	fmt.Println("Q2 RESPONSE:"+response)//Print to console what we're returning
	
}

func q3(w http.ResponseWriter, r *http.Request){
	userId := r.URL.Query()["userid"][0]
	fmt.Println("Q3 REQUEST: with userid="+userId)
	response := TEAM_ID+","+AWS_ACCOUNT_ID+"\n"	
	if mysql {
		response += q3mysql(userId)
	}else{
		response += q3hbase(userId)
	}			
	fmt.Fprintf(w, response)
	fmt.Println("Q3 RESPONSE:"+response)
}
/*
* The server attaches handlers and listens for REST requests on port 80
*/
func main(){
	q2hbaseVar := os.Getenv("Q2HBASE_SERVER")
	q3hbaseVar := os.Getenv("Q3HBASE_SERVER")
	mysqlVar := os.Getenv("MYSQL_SERVER")
	if mysqlVar != ""{
		mysql = true
	}
	
	fmt.Println("Frontend starting using "+backend()+" for the backend...")
	if mysql {
		dsn := dsnFront+mysqlVar+dsnBack 
		var err error
		db, err = sql.Open("mysql", dsn);
		if err != nil {
			log.Fatal(err)
		}
		if err = db.Ping(); err != nil { 
			log.Fatal(err)			
		}else{
			fmt.Println("Database open!")
		}	
	}else{
		q2hbaseServer = "http://"+q2hbaseVar+":8080"
		q3hbaseServer = "http://"+q3hbaseVar+":8080"
	}
  	http.HandleFunc("/q1", q1)
	http.HandleFunc("/q2", q2)
	http.HandleFunc("/q3", q3)
  	log.Fatal(http.ListenAndServe(":80", nil))
}

func backend()(str string){
	if mysql{
		return "MySQL"
	}else{
		return "HBase"
	}
}

/*
* Implementation for Q2 MySQL backend
*/
func q2mysql(userId string, tweetTime string) (response string){
	var tweetId uint64		
	//Find tweet_id for given userid and tweettime		
	rows, err := db.Query("SELECT id FROM tweets WHERE userid='"+userId+"' and created_at='"+tweetTime+"' ORDER BY id;")	
	
	if err != nil {
		log.Print(err)	
		response= err.Error()	
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
* Implementation for Q2 sHBase backend
*/
 func q2hbase(userId string, tweetTime string) (response string){	
 	//Send GET request to HBase Stargate server
 	res, err := http.Get(q2hbaseServer+"/tweets_q2/"+userId+tweetTime+",/about_tweet")

	if err != nil {
 		log.Print(err) 
 		response = err.Error()
 		return response		
 	}// No error, read the response into tweetIds
 	tweetIds, err := ioutil.ReadAll(res.Body)
 	res.Body.Close()
	if err != nil {
		log.Print(err)
		response = err.Error()
		return response
	}// No error, split the tweetIds on ";" and concatenate to response
	results := strings.Split(string(tweetIds), ";")
	for _, id := range results{
		response += (strings.TrimSpace(id)+"\n")
	}
	return response
}

func q3mysql(userId string) (response string){
	var srcId uint64	
	rows, err := db.Query("SELECT src_uid FROM retweets WHERE target_uid='"+userId+"' ORDER BY src_uid;")	
	
	if err != nil {
		log.Print(err)	
		response="Error with MySQL Query for"+userId
	}else{	
		//Grab the data from the  query
		for rows.Next(){
			err = rows.Scan(&srcId)
			if err != nil {
				log.Print(err)				
			}else{//no error, convert the tweet_id into a string and concat to resp				
				response += (strconv.FormatUint(srcId,10)+"\n")
			}
		}
		//Catch lingering errors
		if err := rows.Err(); err != nil {
            log.Print(err)	
    	}			
	}	
	return response
}

func q3hbase(userId string) (response string){
	//Send GET request to HBase Stargate server
 	res, err := http.Get(q3hbaseServer+"/tweets_q3/"+userId+",/about_tweet")

	if err != nil {
 		log.Print(err) 
 		return err.Error() 			
 	}// No error, read the response into tweetIds
 	userIds, err := ioutil.ReadAll(res.Body)
 	res.Body.Close()
	if err != nil {
		log.Print(err)
		response = err.Error()
		return response
	}// No error, split the tweetIds on ";" and concatenate to response
	results := strings.Split(string(userIds), ";")
	for _, id := range results{
		response += (strings.TrimSpace(id)+"\n")
	}
	return response
}
