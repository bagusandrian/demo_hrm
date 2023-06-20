package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/rueian/rueidis"

	hrm "github.com/bagusandrian/hitratemechanism"
)

const (
	KeyPrimary = "product_active"
	Prefix     = "hitrate"
	// HostRedis  = "localhost:6379"
	HostRedis     = "slash-price-v6-redis-cluster.service.gcp-infra-staging-asia-southeast1-staging.consul:6379"
	HostNameRedis = "slash-price-v6-redis-cluster.service.gcp-infra-staging-asia-southeast1-staging.consul"
	UserNameRedis = "default"
	// UserNameRedis = ""
	PasswordRedis = "shcZZP8E2wBmMVnFRSsh"
	// PasswordRedis = ""
	RedisDBName = "local"
)

var (
	client rueidis.Client
)

type Fields struct {
	ProductID     int64
	OriginalPrice float64
	StartDate     string
	EndDate       string
}

func init() {
	ctx := context.Background()
	var err error
	client, err = rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{HostRedis},
		Username:    UserNameRedis,
		Password:    PasswordRedis,
	})
	if err != nil {
		panic(err)
	}
	opt := hrm.Options{
		MaxActiveConn: 100,
		MaxIdleConn:   10,
		Timeout:       3,
		Wait:          true,
		Password:      PasswordRedis,
		Username:      UserNameRedis,
	}
	// checkStringForIpOrHostname(HostNameRedis)
	hrm.New(RedisDBName, HostRedis, "tcp", opt)
	hrm.NewCluster(HostNameRedis, "tcp", []string{}, opt)
	buildRedis(ctx)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

}
func main() {
	ctx := context.Background()
	http.HandleFunc("/dummy-api", func(w http.ResponseWriter, r *http.Request) {
		resp, err := DummyAPI(ctx)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(resp)

	})
	http.HandleFunc("/cluster-testing", func(w http.ResponseWriter, r *http.Request) {
		err := hrm.Pool.ClusterSetex(ctx, "cluster", "testing", "njajal", 100)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)

	})
	log.Println("running server on port :8080")
	http.ListenAndServe("127.0.0.1:8080", nil)
}

func DummyAPI(ctx context.Context) (response Fields, err error) {
	key := genKey()
	reqHrm := hrm.ReqCustomHitRate{
		Config: hrm.ConfigCustomHitRate{
			RedisDBName:     RedisDBName,
			ExtendTTLKey:    60,
			ParseLayoutTime: "2006-01-02 15:04:05 Z0700 MST",
		},
		Threshold: hrm.ThresholdCustomHitrate{
			LimitMaxTTL: 300,
			MaxRPS:      1,
		},
		AttributeKey: hrm.AttributeKeyhitrate{
			KeyCheck: genKey(),
			Prefix:   "hitrate",
		},
	}
	respHrm := hrm.Pool.CustomHitRate(ctx, reqHrm)
	if respHrm.Err != nil {
		log.Println("failed jumpin on custom hitrate")
	}
	// logic for highTraffic
	redisResult := make(map[string]string)
	log.Println("HIGH TRAFFIC indicated:", respHrm.HighTraffic, "| RPS:", respHrm.RPS)
	if respHrm.HighTraffic {
		// HGETALL myhash
		resp := client.DoCache(ctx, client.B().Hgetall().Key(key).Cache(), (30 * time.Second))
		// log.Println(resp.IsCacheHit()) // false
		// log.Println(resp.AsStrMap())   // map[f:v]
		redisResult, err = resp.AsStrMap()
		if err != nil {
			log.Println("error bos")
			return
		}
	} else {
		redisResult, err = hrm.Pool.HgetAll(ctx, RedisDBName, key)
	}
	if len(redisResult) == 0 {
		buildRedis(ctx)
		redisResult, err = hrm.Pool.HgetAll(ctx, RedisDBName, key)
		if err != nil {
			log.Println("err build redis:", err)
			return
		}
	}
	response.ProductID, _ = strconv.ParseInt(redisResult["product_id"], 10, 64)
	response.OriginalPrice, _ = strconv.ParseFloat(redisResult["original_price"], 64)
	response.StartDate = redisResult["start_time"]
	response.EndDate = redisResult["end_time"]
	if !respHrm.HaveMaxDateTTL {
		endTime, err := time.Parse("2006-01-02 15:04:05 Z0700 MST", response.EndDate)
		if err != nil {
			log.Println("err", err)
			return response, err
		}
		hrm.Pool.SetMaxTTLChecker(ctx, RedisDBName, Prefix, key, endTime)
	}
	return
}

func buildRedis(ctx context.Context) {
	// add time sleep for simulate get from DB
	log.Println("get from db")
	for i := 1; i <= 1; i++ {
		data := make(map[string]map[string]interface{})
		key := fmt.Sprintf("%s+%d", KeyPrimary, i)
		data[key] = make(map[string]interface{})
		data[key]["product_id"] = i
		data[key]["original_price"] = float64(12000)
		data[key]["start_time"] = time.Now().Format("2006-01-02 15:04:05 Z0700 MST")
		data[key]["end_time"] = time.Now().Add(6 * time.Minute).Format("2006-01-02 15:04:05 Z0700 MST")

		err := hrm.Pool.HmsetWithExpMultiple(ctx, RedisDBName, data, 100)
		if err != nil {
			log.Println("error", err)
		}
	}
}

func genKey() (key string) {
	key = fmt.Sprintf("%s+%d", KeyPrimary, 1)
	return
}

func checkStringForIpOrHostname(host string) {
	addr := net.ParseIP(host)
	if addr == nil {
		fmt.Println("Given String is a Domain Name")

	} else {
		fmt.Println("Given String is a Ip Address")
	}
	njajal, _ := net.LookupHost(host)
	log.Println(njajal)
}
