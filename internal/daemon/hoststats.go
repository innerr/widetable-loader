package daemon

import (
	"bufio"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// HostStats is one snapshot of the node's load, read from /proc only
// (no external tools needed).
type HostStats struct {
	CPUBusyPct float64 `json:"cpu_busy_pct"`
	CPUs       int     `json:"cpus"`
	LoadAvg1   float64 `json:"load_avg_1"`
	MemTotalKB uint64  `json:"mem_total_kb"`
	MemAvailKB uint64  `json:"mem_avail_kb"`
	DiskIOPS   float64 `json:"disk_iops"` // whole block devices, excluding partitions
	DiskKBs    float64 `json:"disk_kb_per_sec"`
	SampleSec  float64 `json:"sample_sec"`
}

type cpuSample struct {
	total, idle, iowait uint64
}

func readCPUSample() (cpuSample, error) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return cpuSample{}, err
	}
	defer f.Close()
	var s cpuSample
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		fields := strings.Fields(line)[1:] // user nice system idle iowait irq softirq steal ...
		vals := make([]uint64, 0, len(fields))
		for _, f := range fields {
			v, _ := strconv.ParseUint(f, 10, 64)
			vals = append(vals, v)
		}
		// Guest times are already included in user/nice.
		if len(vals) > 8 {
			vals = vals[:8]
		}
		for _, v := range vals {
			s.total += v
		}
		if len(vals) > 3 {
			s.idle = vals[3]
		}
		if len(vals) > 4 {
			s.iowait = vals[4] // busy = total - idle - iowait, same as mpstat
		}
	}
	return s, sc.Err()
}

type diskSample struct {
	reads, writes, rKB, wKB uint64
}

func readDiskSample() (diskSample, error) {
	f, err := os.Open("/proc/diskstats")
	if err != nil {
		return diskSample{}, err
	}
	defer f.Close()
	var s diskSample
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		// major minor name reads ... sectors-written ...
		if len(fields) < 14 {
			continue
		}
		name := fields[2]
		if strings.HasPrefix(name, "loop") || strings.HasPrefix(name, "ram") ||
			strings.HasPrefix(name, "dm-") || strings.HasPrefix(name, "md") {
			continue
		}
		if _, err := os.Stat(filepath.Join("/sys/block", name)); err != nil {
			continue
		}
		s.reads += atoui(fields[3])
		s.rKB += atoui(fields[5]) / 2 // sectors -> KB
		s.writes += atoui(fields[7])
		s.wKB += atoui(fields[9]) / 2
	}
	return s, sc.Err()
}

func atoui(s string) uint64 {
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}

func readMemInfo() (total, avail uint64) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			total = atoui(fields[1])
		case "MemAvailable:":
			avail = atoui(fields[1])
		}
	}
	return total, avail
}

func readLoadAvg() float64 {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(b))
	if len(f) == 0 {
		return 0
	}
	v, _ := strconv.ParseFloat(f[0], 64)
	return v
}

// sampleHostStats takes two CPU/disk samples separated by interval and
// returns one HostStats.
func sampleHostStats(interval time.Duration) HostStats {
	c0, _ := readCPUSample()
	d0, _ := readDiskSample()
	time.Sleep(interval)
	c1, _ := readCPUSample()
	d1, _ := readDiskSample()

	var hs HostStats
	if c1.total > c0.total {
		dt := float64(c1.total - c0.total)
		idle := math.Max(0, float64(c1.idle)-float64(c0.idle))
		wait := math.Max(0, float64(c1.iowait)-float64(c0.iowait))
		hs.CPUBusyPct = math.Max(0, 100*(dt-idle-wait)/dt)
	}
	hs.SampleSec = interval.Seconds()
	hs.CPUs = numCPUs()
	hs.LoadAvg1 = readLoadAvg()
	hs.MemTotalKB, hs.MemAvailKB = readMemInfo()
	if hs.SampleSec > 0 {
		hs.DiskIOPS = float64(d1.reads+d1.writes-d0.reads-d0.writes) / hs.SampleSec
		hs.DiskKBs = float64(d1.rKB+d1.wKB-d0.rKB-d0.wKB) / hs.SampleSec
	}
	return hs
}

func numCPUs() int {
	n := 0
	f, err := os.Open("/proc/stat")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "cpu") && !strings.HasPrefix(sc.Text(), "cpu ") {
			n++
		}
	}
	return n
}

func (this *Server) handleHostStats(w http.ResponseWriter, r *http.Request) {
	interval := time.Second
	if v := r.URL.Query().Get("sec"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || math.IsNaN(f) || f <= 0 || f > 10 {
			writeErr(w, 400, "sec must be in (0,10]")
			return
		}
		interval = time.Duration(f * float64(time.Second))
	}
	writeJSON(w, http.StatusOK, sampleHostStats(interval))
}
