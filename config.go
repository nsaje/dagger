package main

import (
	"errors"
	"net"
)

// Config is used to configure the node
type Config struct {
	RPCAdvertise *net.TCPAddr
}

// DefaultConfig provides default config values
func DefaultConfig() *Config {
	externalIPStr, err := externalIP()
	if err != nil {
		die("Unable to figure out an IP address to bind to.")
	}
	conf := &Config{
		RPCAdvertise: &net.TCPAddr{IP: net.ParseIP(externalIPStr), Port: 8435},
	}
	return conf
}

func externalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}
	return "", errors.New("are you connected to the network?")
}
