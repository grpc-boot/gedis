package gedis

type Option struct {
	Host                  string `yaml:"host" json:"host"`
	Port                  int    `yaml:"port" json:"port"`
	Auth                  string `yaml:"auth" json:"auth"`
	Db                    uint8  `yaml:"db" json:"db"`
	MaxConnLifetimeSecond int    `yaml:"maxConnLifetimeSecond" json:"maxConnLifetimeSecond"`
	IdleTimeoutSecond     int    `yaml:"idleTimeoutSecond" json:"idleTimeoutSecond"`
	MaxIdle               int    `yaml:"maxIdle" json:"maxIdle"`
	MaxActive             int    `yaml:"maxActive" json:"maxActive"`
	Wait                  bool   `yaml:"wait" json:"wait"`
	//单位ms
	ConnectTimeout int `yaml:"connectTimeout" json:"connectTimeout"`
	//单位ms
	ReadTimeout int `yaml:"readTimeout" json:"readTimeout"`
	//单位ms
	WriteTimeout int `yaml:"writeTimeout" json:"writeTimeout"`
	//节点索引
	Index int `yaml:"index" json:"index"`
}

type GroupOption struct {
	Option       Option `yaml:"option" json:"option"`
	VirtualCount int    `yaml:"virtualCount" json:"virtualCount"`
}

func DefaultOption() Option {
	return Option{
		Host:                  "127.0.0.1",
		Port:                  6379,
		Db:                    0,
		MaxConnLifetimeSecond: 1200,
		IdleTimeoutSecond:     60,
		MaxIdle:               2,
		MaxActive:             8,
		ConnectTimeout:        1000,
		ReadTimeout:           3000,
		WriteTimeout:          3000,
	}
}
