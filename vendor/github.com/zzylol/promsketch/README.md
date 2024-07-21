## For Alan to test UnivMon performance
### Install Go
```
wget https://go.dev/dl/go1.22.4.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.22.4.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```
### Get PromSketch code
```
git clone git@github.com:zzylol/promsketch.git
git checkout -b microbenchmark origin/microbenchmark
cd promsketch
export GOPRIVATE=github.com/zzylol/*
go mod tidy
```

### Run SHUniv and EHUniv performance test
```
go test -v -timeout 0 -run ^TestSHUnivPool$ github.com/zzylol/promsketch
```
```
go test -v -timeout 0 -run ^TestUnivPool$ github.com/zzylol/promsketch
```


## Golang test command
```
go test -v manager_test.go manager.go rule.go functions.go sketches.go SmoothHistogram.go ExponentialHistogram.go  heap.go UnivMon.go CountMinSketch.go CountSketch.go utils.go  value.go
```

## Use AVX512 
```
$ grep -q avx512 /proc/cpuinfo && echo "yes, I have AVX512"
yes, I have AVX512

export GOBIN=/usr/local/bin
sudo apt update
sudo apt install clang libc6-dev-i386
go install github.com/gorse-io/goat@latest
git clone https://github.com/gorse-io/goat.git
cd promsketch
goat avx_add_to_int64.c -O3 -mavx -mfma -mavx512f -mavx512dq
mv avx_add_to_int64.c csrc
```
