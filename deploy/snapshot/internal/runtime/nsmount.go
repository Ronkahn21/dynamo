package runtime

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// OpenCheckpointTree creates a detached clone of the mount subtree rooted at
// path (the agent's own checkpoint dir) and returns it as an *os.File so it can
// be handed to a child process via exec.Cmd.ExtraFiles.
//
// The clone is an anonymous mount (attached to no mount namespace), which is the
// only thing that can be grafted into a different mount namespace. A plain bind
// of /proc/self/fd/N fails with EINVAL from inside another namespace because the
// source mount belongs to this (the agent's) namespace; open_tree(OPEN_TREE_CLONE)
// plus move_mount is the kernel API built for exactly this transfer (Linux 5.2+).
func OpenCheckpointTree(path string) (*os.File, error) {
	fd, err := unix.OpenTree(unix.AT_FDCWD, path,
		uint(unix.OPEN_TREE_CLONE|unix.AT_RECURSIVE|unix.OPEN_TREE_CLOEXEC))
	if err != nil {
		return nil, fmt.Errorf("open_tree(%s): %w", path, err)
	}
	return os.NewFile(uintptr(fd), "checkpoint-tree:"+path), nil
}

// AttachCheckpointTree grafts the detached mount referred to by treeFD onto
// target inside the CURRENT mount namespace, creating target if absent. It
// returns a cleanup func that lazily detaches the mount. It is meant to run from
// inside the target container's mount namespace (i.e. from nsrestore), so the
// checkpoint dir becomes visible to CRIU without the workload pod ever mounting
// the PVC.
func AttachCheckpointTree(treeFD int, target string) (func(), error) {
	if err := os.MkdirAll(target, 0o755); err != nil {
		return nil, fmt.Errorf("create checkpoint mount target %s: %w", target, err)
	}
	if err := unix.MoveMount(treeFD, "", unix.AT_FDCWD, target,
		unix.MOVE_MOUNT_F_EMPTY_PATH); err != nil {
		return nil, fmt.Errorf("move_mount -> %s: %w", target, err)
	}
	return func() { _ = unix.Unmount(target, unix.MNT_DETACH) }, nil
}
