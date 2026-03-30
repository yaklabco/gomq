//go:build windows

package broker

// freeDiskBytes returns the number of free bytes available on the
// filesystem containing the given path. On Windows, this is not yet
// implemented and always returns max int64 (effectively disabling
// the disk space check).
func freeDiskBytes(_ string) (int64, error) {
	return 1<<62 - 1, nil //nolint:mnd // sentinel value disabling check
}
