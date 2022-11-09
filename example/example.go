// Copyright 2022 Beijing Volcanoengine Technology Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"

	"github.com/abiosoft/ishell"

	"github.com/volcengine/vegraph-go-sdk/client"
)

type CmdOptions struct {
	Host     string
	Port     string
	AuthPort int
	UserName string
	Password string
	Table    string
}

var options = &CmdOptions{}

var cli *client.Client
var connection client.Option

func (cmdOpts *CmdOptions) RegisterFlags() {
	flag.StringVar(&cmdOpts.Host, "host", "localhost", "graph server hostname")
	flag.StringVar(&cmdOpts.Port, "port", "6283", "graph server port")
	flag.IntVar(&cmdOpts.AuthPort, "auth_port", 6287, "graph server auth port")
	flag.StringVar(&cmdOpts.UserName, "username", "", "username")
	flag.StringVar(&cmdOpts.Password, "password", "", "password")
	flag.StringVar(&cmdOpts.Table, "table", "", "table name")
	flag.Parse()
}

func init() {
	options.RegisterFlags()
}

func instanceID2ServiceName(instID string) string {
	return instID + "-bgdb.bytegraph"
}

func execQuery(query string) {
	if cli == nil {
		fmt.Printf("Submit(%v) failed: cli is nil\n", query)
	}
	result, err := cli.Submit(context.Background(), query, options.Table)
	if err != nil {
		fmt.Printf("Submit(%v) failed: %v\n", query, err)
	} else {
		fmt.Printf("Result: %v\n", result)
	}
}

func defaultDataGenerate() {
	queries := []string{
		`g.addV().property('id', 1).property('type', 1001).property('name', '段正淳').property('power', 60)`,
		`g.addV().property('id', 2).property('type', 1001).property('name', '阿朱').property('power', 5)`,
		`g.addV().property('id', 3).property('type', 1001).property('name', '王语嫣').property('power', 2)`,
		`g.addV().property('id', 4).property('type', 1001).property('name', '段延庆').property('power', 75)`,
		`g.addV().property('id', 5).property('type', 1001).property('name', '乔峰').property('power', 90)`,
		`g.addV().property('id', 6).property('type', 1001).property('name', '段誉').property('power', 80)`,
		`g.addV().property('id', 7).property('type', 1001).property('name', '叶二娘').property('power', 40)`,
		`g.addV().property('id', 8).property('type', 1001).property('name', '虚竹').property('power', 85)`,
		`g.addE('relatives').from(1, 1001).to(2, 1001).property('relation', '父女')`,
		`g.addE('relatives').from(1, 1001).to(3, 1001).property('relation', '父女')`,
		`g.addE('relatives').from(1, 1001).to(4, 1001).property('relation', '兄弟')`,
		`g.addE('relatives').from(2, 1001).to(5, 1001).property('relation', '夫妻')`,
		`g.addE('relatives').from(3, 1001).to(6, 1001).property('relation', '夫妻')`,
		`g.addE('relatives').from(4, 1001).to(6, 1001).property('relation', '父子')`,
		`g.addE('relatives').from(4, 1001).to(7, 1001).property('relation', '义妹')`,
		`g.addE('relatives').from(5, 1001).to(6, 1001).property('relation', '结拜兄弟')`,
		`g.addE('relatives').from(6, 1001).to(8, 1001).property('relation', '结拜兄弟')`,
		`g.addE('relatives').from(8, 1001).to(5, 1001).property('relation', '结拜兄弟')`,
		`g.addE('relatives').from(7, 1001).to(8, 1001).property('relation', '母子')`,
	}

	for _, query := range queries {
		execQuery(query)
	}
}

func main() {
	shell := ishell.New()
	shell.Println("veGraph Sample Interactive Shell")
	shell.Println("Use help to display help")

	shell.AddCmd(&ishell.Cmd{
		Name: "info",
		Help: "展示当前 veGraph 连接信息",
		Func: func(c *ishell.Context) {
			c.Println("veGraph Host: ", options.Host)
			c.Println("veGraph Port: ", options.Port)
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "connect",
		Func: func(c *ishell.Context) {
			if len(c.Args) < 2 {
				c.Err(fmt.Errorf("connect 的参数少于 2 个 "))
				return
			}
			options.Host = c.Args[0]
			options.Port = c.Args[1]

			connection = client.WithHostPort(fmt.Sprintf("%v:%v", options.Host, options.Port))
		},
		Help: "<host> <port>: connect to a veGraph Server",
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "table",
		Func: func(c *ishell.Context) {
			if len(c.Args) < 1 {
				c.Err(fmt.Errorf("table 的参数少于 1 个 "))
				return
			}
			options.Table = c.Args[0]
		},
		Help: "<table name> : select a veGraph table",
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "inst",
		Func: func(c *ishell.Context) {
			if len(c.Args) < 2 {
				c.Err(fmt.Errorf("instance 的参数少于 2 个 "))
				return
			}
			options.Table = c.Args[0]
			options.Host = instanceID2ServiceName(c.Args[0])
			options.Port = c.Args[1]
			port, err := strconv.Atoi(options.Port)
			if err != nil {
				c.Err(fmt.Errorf("port must be an integer"))
				return
			}
			connection = client.WithServiceNamePort(options.Host, port)

		},
		Help: "<instance id> <port>: select a veGraph instance",
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "auth",
		Func: func(c *ishell.Context) {
			if len(c.Args) < 2 {
				c.Err(fmt.Errorf("auth 的参数少于 2 个 "))
				return
			}
			options.UserName = c.Args[0]
			options.Password = c.Args[1]
			var err error
			cli, err = client.NewClient(connection,
				client.WithUserPwd(options.UserName, options.Password),
				client.WithAuthPort(options.AuthPort))

			if cli == nil {
				fmt.Printf("NewClient failed: %v\n", err)
			}
		},
		Help: "<username> <password>: authentication to a veGraph Server",
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "encrypted",
		Func: func(c *ishell.Context) {
			if len(c.Args) < 2 {
				c.Err(fmt.Errorf("encrypted 的参数少于 2 个 "))
				return
			}
			options.UserName = c.Args[0]
			options.Password = c.Args[1]
			var err error
			cli, err = client.NewClient(
				client.WithHostPort(fmt.Sprintf("%v:%v", options.Host, options.Port)), client.WithUserPwdEncrypted(options.UserName, options.Password),
				client.WithAuthPort(options.AuthPort))

			if cli == nil {
				fmt.Printf("NewClient failed: %v\n", err)
			}

		},
		Help: "<username> <password>: authentication to a veGraph Server with encrypted password",
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "auth_port",
		Func: func(c *ishell.Context) {
			var err error
			options.AuthPort, err = strconv.Atoi(c.Args[0])
			if err != nil {
				c.Err(fmt.Errorf("auth port is not a integer: %v", err))
			}
		},
		Help: "auth port",
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "default",
		Func: func(*ishell.Context) {
			defaultDataGenerate()
		},
		Help: "generate default data",
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "query",
		Func: func(c *ishell.Context) {
			if len(c.RawArgs) < 2 {
				c.Err(fmt.Errorf("query 的参数少于 2 个 "))
				return
			}

			str := ""
			for i := 1; i < len(c.RawArgs); i++ {
				str += c.RawArgs[i]
			}
			execQuery(str)
		},
		Help: "execute query",
	})

	shell.Run()
}
