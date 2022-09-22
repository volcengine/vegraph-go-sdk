package client

import (
	"context"
	"testing"

	"github.com/cloudwego/kitex/client/callopt"
	json "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/volcengine/vegraph-go-sdk/kitex_gen/bytegraph"
	"github.com/volcengine/vegraph-go-sdk/structure"
)

func TestSubmit(t *testing.T) {
	ctx := context.Background()
	cli, err := NewClient(WithHostPort("ip:port"), WithUserPwd("user", "passwd"))
	assert.True(t, err == nil)
	// 用mock 的client代替thrift client
	cli.setklient(&TMockedClient{})
	// 跳过鉴权
	authCli := &MockedAuthClient{}
	cli.setAuthClient(authCli)

	elements, err := cli.Submit(ctx, "g.V().has('id',1).has('type',1002).outE('like')", "test")
	assert.True(t, err == nil)
	list, ok := elements.(structure.List)
	assert.True(t, ok)
	assert.True(t, len(list) == 1)
	edge := list[0].(*structure.Edge)
	assert.True(t, edge.OutV.Id == 1)
	assert.True(t, edge.InV.Id == 2)
	assert.True(t, edge.Type == "like")
}

// mock的thrift client
type TMockedClient struct{}

func (c *TMockedClient) GremlinQuery(ctx context.Context, req *bytegraph.GremlinQueryRequest, callOptions ...callopt.Option) (*bytegraph.GremlinQueryResponse, error) {
	jsonResp := `{"errCode":0,"desc":"","retPB":null,"batchRet":null,"batchDesc":[""],"batchErrCode":[0],"batchBinaryRet":["AQENAAAAAQoAAAAEbGlrZQAAAAAAAAABAAAD6gAAAAAAAAACAAAD6g=="],"txnIds":["85b5862c-1ed7-11ed-8822-acde48001122"],"txnTss":[1660814652842142],"txnId":null,"txnTs":null,"costs":[0],"BaseResp":{"StatusMessage":"","StatusCode":0,"Extra":{"IsMaster":"","idc":"boe"}}}`
	tResp := &bytegraph.GremlinQueryResponse{}
	json.Unmarshal([]byte(jsonResp), tResp)
	return tResp, nil
}

type MockedAuthClient struct{}

func (c *MockedAuthClient) UserName() string {
	return "username"
}

func (c *MockedAuthClient) Password() string {
	return "passwd"
}
func (c *MockedAuthClient) Session(bool) (string, error) {
	return "session_xx", nil
}
