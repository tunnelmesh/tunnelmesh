package nfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/tunnelmesh/tunnelmesh/internal/coord/s3"
)

func newTestStore(t *testing.T) *s3.Store {
	t.Helper()
	masterKey := [32]byte{1, 2, 3, 4, 5, 6, 7, 8}
	store, err := s3.NewStoreWithCAS(t.TempDir(), nil, masterKey)
	if err != nil {
		t.Fatalf("NewStoreWithCAS failed: %v", err)
	}
	return store
}

func TestS3Filesystem_NormalizePath(t *testing.T) {
	store := newTestStore(t)
	_ = store.CreateBucket(context.Background(), "test-bucket", "", 2)

	tests := []struct {
		name     string
		prefix   string
		path     string
		expected string
	}{
		{"no prefix, root", "", "", ""},
		{"no prefix, simple path", "", "file.txt", "file.txt"},
		{"no prefix, nested path", "", "dir/file.txt", "dir/file.txt"},
		{"no prefix, leading slash", "", "/file.txt", "file.txt"},
		{"with prefix, root", "prefix", "", "prefix"},
		{"with prefix, simple path", "prefix", "file.txt", "prefix/file.txt"},
		{"with prefix, nested path", "prefix", "dir/file.txt", "prefix/dir/file.txt"},
		{"with prefix, leading slash", "prefix", "/file.txt", "prefix/file.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := NewS3Filesystem(store, "test-bucket", tt.prefix, false)
			got := fs.normalizePath(tt.path)
			if got != tt.expected {
				t.Errorf("normalizePath(%q) = %q, want %q", tt.path, got, tt.expected)
			}
		})
	}
}

func TestS3Filesystem_CreateAndRead(t *testing.T) {
	store := newTestStore(t)
	_ = store.CreateBucket(context.Background(), "test-bucket", "", 2)
	fs := NewS3Filesystem(store, "test-bucket", "", false)

	// Create a file
	f, err := fs.Create("test.txt")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write to file
	content := []byte("hello world")
	n, err := f.Write(content)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(content) {
		t.Errorf("Write returned %d, want %d", n, len(content))
	}

	// Close to flush
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read the file back
	f2, err := fs.Open("test.txt")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = f2.Close() }()

	data, err := io.ReadAll(f2)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Errorf("Read content = %q, want %q", data, content)
	}
}

func TestS3Filesystem_ReadOnly(t *testing.T) {
	store := newTestStore(t)
	_ = store.CreateBucket(context.Background(), "test-bucket", "", 2)
	fs := NewS3Filesystem(store, "test-bucket", "", true)

	// Try to create - should fail
	_, err := fs.Create("test.txt")
	if !errors.Is(err, errReadOnly) {
		t.Errorf("Create on read-only fs returned %v, want errReadOnly", err)
	}

	// Try to rename
	err = fs.Rename("a.txt", "b.txt")
	if !errors.Is(err, errReadOnly) {
		t.Errorf("Rename on read-only fs returned %v, want errReadOnly", err)
	}

	// Try to remove
	err = fs.Remove("test.txt")
	if !errors.Is(err, errReadOnly) {
		t.Errorf("Remove on read-only fs returned %v, want errReadOnly", err)
	}

	// Try to TempFile
	_, err = fs.TempFile("", "prefix")
	if !errors.Is(err, errReadOnly) {
		t.Errorf("TempFile on read-only fs returned %v, want errReadOnly", err)
	}
}

func TestS3Filesystem_Stat(t *testing.T) {
	store := newTestStore(t)
	_ = store.CreateBucket(context.Background(), "test-bucket", "", 2)
	fs := NewS3Filesystem(store, "test-bucket", "", false)

	// Create a file
	content := []byte("test content")
	_, _ = store.PutObject(context.Background(), "test-bucket", "myfile.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)

	// Stat the file
	info, err := fs.Stat("myfile.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if info.Name() != "myfile.txt" {
		t.Errorf("Name = %q, want %q", info.Name(), "myfile.txt")
	}
	if info.Size() != int64(len(content)) {
		t.Errorf("Size = %d, want %d", info.Size(), len(content))
	}
	if info.IsDir() {
		t.Error("IsDir = true, want false")
	}

	// Stat non-existent file
	_, err = fs.Stat("nonexistent.txt")
	if !os.IsNotExist(err) {
		t.Errorf("Stat nonexistent returned %v, want os.ErrNotExist", err)
	}
}

func TestS3Filesystem_ReadDir(t *testing.T) {
	store := newTestStore(t)
	_ = store.CreateBucket(context.Background(), "test-bucket", "", 2)
	fs := NewS3Filesystem(store, "test-bucket", "", false)

	// Create some files
	_, _ = store.PutObject(context.Background(), "test-bucket", "file1.txt", bytes.NewReader([]byte("a")), 1, "text/plain", nil)
	_, _ = store.PutObject(context.Background(), "test-bucket", "file2.txt", bytes.NewReader([]byte("bb")), 2, "text/plain", nil)
	_, _ = store.PutObject(context.Background(), "test-bucket", "subdir/file3.txt", bytes.NewReader([]byte("ccc")), 3, "text/plain", nil)

	// Read root dir
	entries, err := fs.ReadDir("")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("ReadDir returned %d entries, want 3", len(entries))
	}

	// Check entries are sorted
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	if names[0] != "file1.txt" || names[1] != "file2.txt" || names[2] != "subdir" {
		t.Errorf("ReadDir entries = %v, want [file1.txt, file2.txt, subdir]", names)
	}

	// subdir should be a directory
	if !entries[2].IsDir() {
		t.Error("subdir IsDir = false, want true")
	}
}

func TestS3Filesystem_Chroot(t *testing.T) {
	store := newTestStore(t)
	_ = store.CreateBucket(context.Background(), "test-bucket", "", 2)

	// Create a file in subdir
	_, _ = store.PutObject(context.Background(), "test-bucket", "subdir/file.txt", bytes.NewReader([]byte("test")), 4, "text/plain", nil)

	fs := NewS3Filesystem(store, "test-bucket", "", false)

	// Chroot to subdir
	chrooted, err := fs.Chroot("subdir")
	if err != nil {
		t.Fatalf("Chroot failed: %v", err)
	}

	// File should be accessible as just "file.txt"
	info, err := chrooted.Stat("file.txt")
	if err != nil {
		t.Fatalf("Stat in chroot failed: %v", err)
	}
	if info.Name() != "file.txt" {
		t.Errorf("Name = %q, want %q", info.Name(), "file.txt")
	}
}

func TestS3Filesystem_MkdirAll(t *testing.T) {
	store := newTestStore(t)
	_ = store.CreateBucket(context.Background(), "test-bucket", "", 2)
	fs := NewS3Filesystem(store, "test-bucket", "", false)

	// MkdirAll should be a no-op (S3 doesn't have real directories)
	err := fs.MkdirAll("a/b/c", 0755)
	if err != nil {
		t.Errorf("MkdirAll failed: %v", err)
	}
}

func TestS3Filesystem_SymlinkNotSupported(t *testing.T) {
	store := newTestStore(t)
	_ = store.CreateBucket(context.Background(), "test-bucket", "", 2)
	fs := NewS3Filesystem(store, "test-bucket", "", false)

	err := fs.Symlink("target", "link")
	if err == nil {
		t.Error("Symlink should return error")
	}

	_, err = fs.Readlink("link")
	if err == nil {
		t.Error("Readlink should return error")
	}
}

func TestS3Filesystem_Join(t *testing.T) {
	store := newTestStore(t)
	fs := NewS3Filesystem(store, "test-bucket", "", false)

	result := fs.Join("a", "b", "c.txt")
	if result != "a/b/c.txt" {
		t.Errorf("Join = %q, want %q", result, "a/b/c.txt")
	}
}

func TestS3Filesystem_Root(t *testing.T) {
	store := newTestStore(t)
	fs := NewS3Filesystem(store, "test-bucket", "prefix", false)

	if fs.Root() != "/" {
		t.Errorf("Root = %q, want %q", fs.Root(), "/")
	}
}

func TestS3File_Seek(t *testing.T) {
	store := newTestStore(t)
	_ = store.CreateBucket(context.Background(), "test-bucket", "", 2)
	fs := NewS3Filesystem(store, "test-bucket", "", false)

	// Create a file
	content := []byte("0123456789")
	_, _ = store.PutObject(context.Background(), "test-bucket", "seek.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)

	f, err := fs.Open("seek.txt")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = f.Close() }()

	// Seek to position 5
	pos, err := f.Seek(5, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek failed: %v", err)
	}
	if pos != 5 {
		t.Errorf("Seek returned %d, want 5", pos)
	}

	// Read from position 5
	buf := make([]byte, 5)
	n, err := f.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("Read failed: %v", err)
	}
	if string(buf[:n]) != "56789" {
		t.Errorf("Read = %q, want %q", string(buf[:n]), "56789")
	}
}

func TestS3File_Truncate(t *testing.T) {
	store := newTestStore(t)
	_ = store.CreateBucket(context.Background(), "test-bucket", "", 2)
	fs := NewS3Filesystem(store, "test-bucket", "", false)

	// Create a file
	content := []byte("hello world")
	_, _ = store.PutObject(context.Background(), "test-bucket", "trunc.txt", bytes.NewReader(content), int64(len(content)), "text/plain", nil)

	f, err := fs.OpenFile("trunc.txt", os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	// Truncate to 5 bytes
	sf := f.(*s3File)
	err = sf.Truncate(5)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Read back
	_ = f.Close()
	reader, meta, err := store.GetObject(context.Background(), "test-bucket", "trunc.txt")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer func() { _ = reader.Close() }()

	if meta.Size != 5 {
		t.Errorf("Size after truncate = %d, want 5", meta.Size)
	}
}

func TestS3FileInfo(t *testing.T) {
	now := time.Now()
	fi := &s3FileInfo{
		name:    "test.txt",
		size:    100,
		mode:    0644,
		modTime: now,
		isDir:   false,
	}

	if fi.Name() != "test.txt" {
		t.Errorf("Name = %q, want %q", fi.Name(), "test.txt")
	}
	if fi.Size() != 100 {
		t.Errorf("Size = %d, want 100", fi.Size())
	}
	if fi.Mode() != 0644 {
		t.Errorf("Mode = %v, want 0644", fi.Mode())
	}
	if !fi.ModTime().Equal(now) {
		t.Errorf("ModTime = %v, want %v", fi.ModTime(), now)
	}
	if fi.IsDir() {
		t.Error("IsDir = true, want false")
	}
	if fi.Sys() != nil {
		t.Errorf("Sys = %v, want nil", fi.Sys())
	}

	// Test directory
	fi2 := &s3FileInfo{
		name:  "dir",
		mode:  0755 | os.ModeDir,
		isDir: true,
	}
	if !fi2.IsDir() {
		t.Error("dir IsDir = false, want true")
	}
}
