package env

import (
	"log"

	"github.com/spf13/viper"
)

type Config struct {
	File string
}

func GetEnvVars() Config {
	var err error
	var config Config
	viper.SetConfigName("config")
	viper.AddConfigPath("./")
	if err = viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			viper.AutomaticEnv()
		}
		val, ok := viper.Get("file").(string)
		if !ok {
			log.Fatalf("Error while reading config file:%s", err)
		}
		config.File = val
	} else {
		viper.Unmarshal(&config)
	}

	return config
}
