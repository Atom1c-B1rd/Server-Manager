package managers

import (
	"fmt"

	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
)

func ValidateGoCode(code string) error {
	i := interp.New(interp.Options{})
	_ = i.Use(stdlib.Symbols)

	_, err := i.Eval(code)
	if err != nil {
		return fmt.Errorf("syntax/eval error: %w", err)
	}
	return nil
}
