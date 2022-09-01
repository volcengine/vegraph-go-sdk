# Volcengine Graph Storage SDK for Go-veGraph

中文 | [English](README.md)

veGraph是火山引擎自研的图存储引擎，veGraph SDK for Go 提供了访问veGraph的标准化接口封装。

# 接入方式
    func TestGremlin(t *testing.T) {
        ctx := context.Background()
        cli, err := client.NewClient("destServiceXXX",
            client.WithHostPort("host:port"),
            WithUserPwd("user", "password"),
            WithDefaultTable("default"),
        )
        assert.True(t, err == nil)
	    elements, err := cli.Submit(ctx, "g.V().has('id',1).has('type',1001).properties()", "default")
	    assert.True(t, err == nil)
        t.Logf("result=%#v", elements)
    }

# 注意事项
1. 推荐使用Go 1.18 及以上版本接入
