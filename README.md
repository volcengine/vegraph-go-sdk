# Volcengine Graph Storage SDK for Go-veGraph

English | [中文](README_cn.md)

VeGraph Go Client provides support for interacting with the graph database veGraph.

## Usage Examples

```
func TestGremlin(t *testing.T) {
    ctx := context.Background()
    cli, err := client.NewClient(
        client.WithHostPort("host:port"),
        WithUserPwd("user", "password"),
        WithDefaultTable("default"),
    )
    assert.True(t, err == nil)
    elements, err := cli.Submit(ctx, "g.V().has('id',1).has('type',1001).properties()", "default")
    assert.True(t, err == nil)
    t.Logf("result=%#v", elements)
}

```

## License
[Apache License 2.0](../LICENSE)

