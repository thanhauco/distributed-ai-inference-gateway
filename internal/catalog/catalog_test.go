package catalog

import "testing"

func TestResolveAndReplace(t *testing.T) {
	c, err := New([]Model{{Name: "llama3-70b", Aliases: []string{"gpt-4o"}, DefaultMaxTokens: 512}}, Limits{TokensPerMin: 10})
	if err != nil {
		t.Fatal(err)
	}
	if m, ok := c.Resolve("gpt-4o"); !ok || m.Name != "llama3-70b" || m.DefaultMaxTokens != 512 {
		t.Fatalf("alias: %+v %v", m, ok)
	}
	if m, ok := c.Resolve("phi3"); ok || m.Name != "phi3" {
		t.Fatalf("unknown passthrough: %+v %v", m, ok)
	}
	if err := c.SetModels([]Model{{Name: "a", Aliases: []string{"x"}}, {Name: "b", Aliases: []string{"x"}}}); err == nil {
		t.Fatal("duplicate alias accepted")
	}
	if _, ok := c.Resolve("gpt-4o"); !ok {
		t.Fatal("failed replace must not clobber the catalog")
	}
	c.SetLimits(Limits{TokensPerMin: 99})
	if c.Limits().TokensPerMin != 99 || len(c.Models()) != 1 {
		t.Fatal("SetLimits must keep models")
	}
}
