package network

import (
	"net"
	"testing"
)

func TestAllocate(t *testing.T) {
	_, ipNet, _ := net.ParseCIDR("192.168.0.1/24")
	ip, err := ipAllocator.Allocate(ipNet)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("alloc ip: %v", ip)
}

func TestRelease(t *testing.T) {
	ip, ipNet, _ := net.ParseCIDR("192.168.0.1/24")
	err := ipAllocator.Release(ipNet, &ip)
	if err != nil {
		t.Fatal(err)
	}
}

func TestAllAllocate(t *testing.T) {
	/*
	   ipam_test.go:44: alloc ip: 192.168.0.1
	   ipam_test.go:44: alloc ip: 192.168.0.2
	   ipam_test.go:44: alloc ip: 192.168.0.3
	   ipam_test.go:44: alloc ip: 192.168.0.252
	   ipam_test.go:44: alloc ip: 192.168.0.253
	   ipam_test.go:44: alloc ip: 192.168.0.254
	   ipam_test.go:42: no available ip in subnet
	*/
	// 测试是否溢出
	for i := 0; i < 256; i++ {
		_, ipNet, _ := net.ParseCIDR("192.168.0.1/24")
		ip, err := ipAllocator.Allocate(ipNet)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("alloc ip: %v", ip)
	}
}
