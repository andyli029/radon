package sqlparser

import (
	"strings"
	"testing"
)

func TestRadonAttach(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{
		// name, address, user, password.
		{
			input:  "radon attach ('attach1', '127.0.0.1:6000', 'root', '123456')",
			output: "radon attach ('attach1', '127.0.0.1:6000', 'root', '123456')",
		},
		{
			input:  "radon list_attach",
			output: "radon list_attach",
		},
		{
			input:  "radon detach('attach1')",
			output: "radon detach ('attach1')",
		},
	}

	for _, exp := range validSQL {
		sql := strings.TrimSpace(exp.input)
		tree, err := Parse(sql)
		if err != nil {
			t.Errorf("input: %s, err: %v", sql, err)
			continue
		}

		// Walk.
		Walk(func(node SQLNode) (bool, error) {
			return true, nil
		}, tree)

		got := String(tree.(*Radon))
		if exp.output != got {
			t.Errorf("want:\n%s\ngot:\n%s", exp.output, got)
		}
	}
}
