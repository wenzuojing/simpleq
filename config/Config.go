package config

import (
	"github.com/revel/config"
)

type SimpleqConfig struct {
	config  *config.Config
	section string
}

func LoadConfig(configPath string) (*SimpleqConfig, error) {

	config, err := config.ReadDefault(configPath)

	if err != nil {
		return nil, err
	}

	return &SimpleqConfig{config, ""}, nil

}

func (c *SimpleqConfig) SetSection(section string) {
	c.section = section
}

func (c *SimpleqConfig) Int(option string) (result int, found bool) {
	result, err := c.config.Int(c.section, option)
	if err == nil {
		return result, true
	} else {
		return 0, false
	}

}

func (c *SimpleqConfig) IntDefault(option string, dfault int) int {
	if r, found := c.Int(option); found {
		return r
	}
	return dfault
}

func (c *SimpleqConfig) Bool(option string) (result, found bool) {
	result, err := c.config.Bool(c.section, option)
	if err == nil {
		return result, true
	} else {
		return false, false
	}
}

func (c *SimpleqConfig) BoolDefault(option string, dfault bool) bool {
	if r, found := c.Bool(option); found {
		return r
	}
	return dfault
}

func (c *SimpleqConfig) String(option string) (result string, found bool) {
	if r, err := c.config.String(c.section, option); err == nil {
		return r, true
	}
	return "", false
}

func (c *SimpleqConfig) StringDefault(option, dfault string) string {
	if r, found := c.String(option); found {
		return r
	}
	return dfault
}
