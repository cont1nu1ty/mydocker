package main

import (
	log "github.com/sirupsen/logrus"
	"mydocker/cgroups"
	"mydocker/cgroups/subsystems"
	"mydocker/container"
	"mydocker/network"
	"os"
	"strconv"
	"strings"
)

// Run 执行具体 command
/*
这里的Start方法是真正开始执行由NewParentProcess构建好的command的调用，它首先会clone出来一个namespace隔离的
进程，然后在子进程中，调用/proc/self/exe,也就是调用自己，发送init参数，调用我们写的init方法，
去初始化容器的一些资源。
*/
func Run(tty bool, comArray, envSlice []string, res *subsystems.ResourceConfig, volume, containerName, imageName string,
    net string, portMapping []string) {
    containerId := container.GenerateContainerID()

    parent, writePipe := container.NewParentProcess(tty, volume, containerId, imageName, envSlice)
    if parent == nil {
        log.Errorf("New parent process error")
        return
    }
    if err := parent.Start(); err != nil {
        log.Errorf("Run parent.Start err:%v", err)
        return
    }

    cgroupManager := cgroups.NewCgroupManager("mydocker-cgroup")
    _ = cgroupManager.Set(res)
    _ = cgroupManager.Apply(parent.Process.Pid, res)

    var containerIP string
    if net != "" {
        containerInfo := &container.Info{
            Id:          containerId,
            Pid:         strconv.Itoa(parent.Process.Pid),
Name:        containerName,
PortMapping: portMapping,
        }
        ip, err := network.Connect(net, containerInfo)
        if err != nil {
            log.Errorf("Error Connect Network %v", err)
            return
        }
        containerIP = ip.String()
    }

    containerInfo, err := container.RecordContainerInfo(parent.Process.Pid, comArray, containerName, containerId,
        volume, net, containerIP, portMapping)
    if err != nil {
        log.Errorf("Record container info error %v", err)
        return
    }

    if err := sendInitCommand(comArray, writePipe); err != nil {
        log.Errorf("Failed to send init command: %v", err)
        return
    }

    // 定义清理
	cleanup := func() {
    log.Info("Starting cleanup...")
    if net != "" {
        network.Disconnect(net, containerInfo)
    }
    cgroupManager.Destroy()
    container.DeleteWorkSpace(containerId, volume)
    container.DeleteContainerInfo(containerId)
    log.Info("Cleanup complete.")
}
    if tty {
        // 前台模式：同步等待并清理
         _,_ = parent.Process.Wait()
		log.Info("-it parent.Process.Wait done.")
        cleanup()
		
     } else {
        // 后台模式：异步等待并清理
        go func() {
            _,_ = parent.Process.Wait()
			log.Info("-d parent.Process.Wait done.")
            cleanup()
        }()
    }
}

// sendInitCommand 通过writePipe将指令发送给子进程
func sendInitCommand(comArray []string, writePipe *os.File) error {
    command := strings.Join(comArray, " ")
    log.Infof("command all is %s", command)
    _, err := writePipe.WriteString(command)
    if err != nil {
        return err  // 返回错误
    }
    err = writePipe.Close()
    if err != nil {
        return err  // 返回关闭管道时的错误
    }
    return nil  // 没有错误时返回 nil
}
