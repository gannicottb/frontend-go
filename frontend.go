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

var dsn = "cloud9:gradproject@tcp(ec2-54-198-107-252.compute-1.amazonaws.com:3306)/TWEET_DB?parseTime=true"
var TEAM_ID, AWS_ACCOUNT_ID = "cloud9", "4897-8874-0242"
var db *sql.DB 
const layout = "2006-01-02 15:04:05"


func heartbeat (w http.ResponseWriter, r *http.Request){
	timeNow := time.Now().Format(layout)
	fmt.Println("Heartbeat at "+timeNow)
        fmt.Fprintf(w, TEAM_ID+","+AWS_ACCOUNT_ID+","+timeNow)
}

/*
* Implementation for MySQL backend
*/
func queryMySQL(userId string, tweetTime string) (response string){
	var tweetId uint64	
	response="No tweet found"
	//Find tweet_id for given userid and tweettime		
	rows, err := db.Query("SELECT id FROM tweets WHERE userid='"+userId+"' and created_at='"+tweetTime+"';")	
	
	if err != nil {
		log.Print(err)		
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
func queryHBase(userid string tweetime string) (){

}
*/

func findTweet (w http.ResponseWriter, r *http.Request){
	//Extract values from URL
	values := r.URL.Query()
	userId := values["userid"][0]
	tweetTime := values["tweet_time"][0]
	fmt.Println("Request with userid="+userId+", tweet_time="+tweetTime)
	
	//Begin response
	response := TEAM_ID+","+AWS_ACCOUNT_ID+"\n"

	// var tweet_id uint64	
	// //Do query		
	// rows, err := db.Query("SELECT id FROM tweets WHERE userid='"+userid[0]+"' and created_at='"+tweettime[0]+"';")	
	
	// if err != nil {
	// 	log.Print(err)		
	// }else{	
	// 	//Grab the data from the  query
	// 	for rows.Next(){
	// 		err = rows.Scan(&tweet_id)
	// 		if err != nil {
	// 			log.Fatal(err)
	// 		}				
	// 		resp += (strconv.FormatUint(tweet_id,10)+"\n")
	// 	}
	// 	fmt.Println(resp)
	// }
	response += queryMySQL(userId, tweetTime)
	//resp += queryHBase(userid, tweettime)
	//Send response
	fmt.Println(response)//Print to console what we're returning
	fmt.Fprintf(w, response)
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


