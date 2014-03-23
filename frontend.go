package main

import(
 "database/sql"
 _"mysql-1.0"
 "net/http" 
 "fmt"
 "log"
 "time"
 "strconv"
)
//Test Query for q2:
//http://ec2-54-85-165-64.compute-1.amazonaws.com:8080/q2?userid=422&tweet_time=2014-02-03%2000:40:09

var dsn = "cloud9:gradproject@tcp(15619projectlb-1895861859.us-east-1.elb.amazonaws.com:3306)/TWEET_DB?parseTime=true"
var TEAM_ID, AWS_ACCOUNT_ID = "cloud9", "4897-8874-0242"
var db *sql.DB 
const layout = "2006-01-02 15:04:05"


func heartbeat (w http.ResponseWriter, r *http.Request){
	timeNow := time.Now().Format(layout)
	fmt.Println("Heartbeat at "+timeNow)
        fmt.Fprintf(w, TEAM_ID+","+AWS_ACCOUNT_ID+","+timeNow)
}

func findTweet (w http.ResponseWriter, r *http.Request){
	//Extract values from URL
	values := r.URL.Query()
	userid := values["userid"]
	tweettime := values["tweet_time"]	
	fmt.Println("Request with user_id="+userid[0]+", tweet_time="+tweettime[0])
	
	//Begin response
	resp := TEAM_ID+","+AWS_ACCOUNT_ID+"\n"
	var tweet_id uint64
	
	//Do query		
	rows, err := db.Query("SELECT id FROM tweets WHERE userid='"+userid[0]+"' and created_at='"+tweettime[0]+"';")	
	
	if err != nil {
		log.Print(err)		
	}else{	
		//Grab the data from the  query
		for rows.Next(){
			err = rows.Scan(&tweet_id)
			if err != nil {
				log.Fatal(err)
			}				
			resp += (strconv.FormatUint(tweet_id,10)+"\n")
		}
		fmt.Println(resp)
	}
	//Send response
	fmt.Fprintf(w, resp)
}

func main(){
	fmt.Printf("Starting up the frontend now...")
	var err error
	db, err = sql.Open("mysql", dsn);
	if err != nil {
		log.Fatal(err)
	}else{
		fmt.Println("Database open!")
	}
    	http.HandleFunc("/q1", heartbeat)
	http.HandleFunc("/q2", findTweet)
    	log.Fatal(http.ListenAndServe(":8080", nil))
}


