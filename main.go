package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/ini.v1"
)

// var choke = make(chan [2]string)

// var f mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
// 	choke <- [2]string{msg.Topic(), string(msg.Payload())}
// }

// Log_Config set where the log whill be output
func Log_Config(log_name string) {
	file, err := os.OpenFile(log_name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	// mqtt.DEBUG = log.New(file, "", log.LstdFlags|log.Llongfile)
	mqtt.ERROR = log.New(file, "", log.LstdFlags|log.Llongfile)

}

// Load_Config load config from *.ini
func Load_Config(cfg_name string) (host, clientid, username, password string) {
	cfg, err := ini.Load(cfg_name)
	if err != nil {
		mqtt.ERROR.Println("Fail to read file: %v", err)
		os.Exit(1)
	}

	host = cfg.Section("mqtt").Key("host").String()
	clientid = cfg.Section("mqtt").Key("clientid").String()
	username = cfg.Section("mqtt").Key("username").String()
	password = cfg.Section("mqtt").Key("password").String()
	return
}

// Connect create the mqtt client and connect to the specific broke by given host
// return mqtt.Client
func Connect(host, clientid, username, password string) (c mqtt.Client) {

	opts := mqtt.NewClientOptions().AddBroker(host).SetClientID(clientid)
	opts.SetKeepAlive(2 * time.Second)
	opts.SetDefaultPublishHandler(
		func(client mqtt.Client, msg mqtt.Message) {
			mqtt.ERROR.Println("TOPIC: %s MSG: %s", msg.Topic(), msg.Payload())
		})
	opts.SetPingTimeout(1 * time.Second)
	opts.SetUsername(username).SetPassword(password)
	c = mqtt.NewClient(opts)

	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	return
}

// Pub Publish the specific topic and mag to the giver broke
func Pub(c mqtt.Client) {
	// Publish data
	type Data struct {
		DeviceID  string            `JSON:"deviceID"`  //设备id
		Timestamp string            `JSON:"timestamp"` //时间戳
		Fields    map[string]string `JSON:"fields"`    //标签
	}

	for i := 0; i < 10; i++ {
		dt := Data{
			DeviceID:  "gotest",
			Timestamp: time.Now().Format("2006-01-02 15:04:05"),
			Fields:    map[string]string{"tag1": "this is msg " + strconv.Itoa(i), "tag2": "god_girl"},
		}
		msg, err := json.Marshal(dt)
		if err != nil {
			mqtt.ERROR.Println("Umarshal failed:", err)
		}

		token := c.Publish("test", 0, false, msg)
		token.Wait()
		time.Sleep(10 * time.Second)

	}
	c.Disconnect(250)
}

// Sub Subscribe from the broke by give host,topic
func Sub(c mqtt.Client, topic string) {
	var choke = make(chan [2]string)

	var f mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
		choke <- [2]string{msg.Topic(), string(msg.Payload())}
	}
	for {
		if token := c.Subscribe(topic, 0, f); token.Wait() && token.Error() != nil {
			mqtt.ERROR.Println(token.Error())
			os.Exit(1)
		}
		for {
			incoming := <-choke
			mqtt.ERROR.Printf("Received:TOPIC: %s\n", incoming[0])
			writeFile(incoming[1])
		}
	}

}

// writeFile write the recevied msg to  specific josn file
func writeFile(v string) {
	// 打开文件
	filePtr, err := os.OpenFile("mqtt.json", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		mqtt.ERROR.Println(err)
	}

	defer filePtr.Close()

	type Data struct {
		DeviceID  string            `JSON:"deviceID"`  //设备id
		Timestamp string            `JSON:"timestamp"` //时间戳
		Fields    map[string]string `JSON:"fields"`    //标签
	}
	var data Data
	if err := json.Unmarshal([]byte(v), &data); err == nil {

		// 创建Json编码器
		encoder := json.NewEncoder(filePtr)
		err = encoder.Encode(data)
		if err != nil {
			mqtt.ERROR.Println("writeFile failed", err.Error())
		} else {
			mqtt.ERROR.Println("writeFile success")
		}
	} else {
		mqtt.ERROR.Println(err)
	}

}

func main() {
	Log_Config("mqtt.log")
	host, clientid, username, password := Load_Config("mqtt.ini")
	c := Connect(host, clientid, username, password)
	Sub(c, "test")
	// Pub(c)
}
