#!/bin/bash
# Usage: ./last10range.sh 10.0.0.0/24

cidr="$1"

# Get the first IP and mask
IFS=/ read ip mask <<< "$cidr"

# Convert IP to integer
ip2int() {
  local a b c d
  IFS=. read a b c d <<< "$1"
  echo "$(( (a << 24) + (b << 16) + (c << 8) + d ))"
}

# Convert integer to IP
int2ip() {
  local ip=$1
  echo "$(( (ip >> 24) & 255 )).$(( (ip >> 16) & 255 )).$(( (ip >> 8) & 255 )).$(( ip & 255 ))"
}

# Calculate number of hosts
hosts=$(( (1 << (32 - mask)) ))

# Calculate network base as int
base=$(ip2int "$ip")

# Calculate first and last usable IPs
first_ip=$(( base + 1 ))
last_ip=$(( base + hosts - 2 ))

# Calculate the range: 10 before the last one
start=$(( last_ip - 10 + 1 ))
end=$(( last_ip ))

echo "$(int2ip $start)-$(int2ip $end)"
