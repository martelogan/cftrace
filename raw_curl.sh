#!/bin/bash

mkdir -p logs cached

url="https://34.49.121.93/internal-echo"
samples=5

curl_format="curl-format.txt"
cat > $curl_format <<EOL
time_namelookup:  %{time_namelookup}\n
time_connect:  %{time_connect}\n
time_appconnect:  %{time_appconnect}\n
time_pretransfer:  %{time_pretransfer}\n
time_starttransfer:  %{time_starttransfer}\n
time_total:  %{time_total}\n
EOL

run_curl() {
    session_log="logs/session_$1.log"
    verbose_log="logs/verbose_$1.log"

    totals=(0.0 0.0 0.0 0.0 0.0 0.0)
    labels=("time_namelookup" "time_connect" "time_appconnect" "time_pretransfer" "time_starttransfer" "time_total")

    echo -e "\n=== $3 ===\n" | tee $session_log

    for (( i=0; i<$samples; i++ )); do
        echo -e "\n=== Iteration $i ===" >> $verbose_log
        output=$(curl -w "@$curl_format" -o /dev/null -s "$url" \
                -H "$2" $4 --compressed -k -L --retry 3 2>> $verbose_log)
        echo "(iteration $i)" >> $session_log
        echo "$output" >> $session_log  # Log raw results for debugging

        count=0
        while IFS=: read -r label value; do
            value=$(echo "$value" | xargs)  # Trim whitespace
            totals[count]=$(echo "${totals[count]} + $value" | bc)  # Sum the values
            ((count++))
        done <<< "$output"
    done

    echo -e "\nAveraged Results (ms):\n" | tee -a $session_log
    for i in {0..5}; do
        avg=$(echo "scale=2; ${totals[i]} / $samples * 1000" | bc)
        printf "%s=%.2fms\n" "${labels[i]}" "$avg" | tee -a $session_log
    done
}

echo -e "\nTesting: Cold TCP & Cold TLS"
run_curl 1 "Connection: close" "Cold TCP & Cold TLS" "-j"

echo -e "\nTesting: Warm TCP & Cold TLS"
run_curl 2 "Connection: keep-alive" "Warm TCP & Cold TLS" "-c cached/cookies_2.txt"

echo -e "\nTesting: Warm TCP & Warm TLS"
run_curl 3 "Connection: keep-alive" "Warm TCP & Warm TLS" "-b cached/cookies_2.txt -c cached/cookies_3.txt"

echo -e "\nAll results are recorded in detailed session logs in the logs directory."
