





#!/bin/bash

# URL for sending SMS
SEND_SMS_URL="https://rest.nexmo.com/sms/json"
API_KEY="0c050de2"
API_SECRET="B22d9caf2c5a8890f6f"
SENDER="jm"
RECIPIENT1="8617698929143"
RECIPIENT2="8617856923766"
RECIPIENT3="8617621535102"
RECIPIENT4="8615693653249"

# Array of custom messages
messages=(
    "【Akamai】 asia-vcode-api.vivoglobal.com 【Akamai CDN+】 vivo-告警，域名：asia-vcode-api.vivoglobal.com发生了5xx状态码超出5%25告警，当前值：40%25。最近一次告警时间：2023-08-19 00:00:00"
    "【Akamai】 asia-vcode-api.vivoglobal.com 【Akamai CDN+】 vivo-告警，域名：asia-vcode-api.vivoglobal.com发生了5xx状态码超出5%25告警，当前值：45%25。最近一次告警时间：2023-08-19 00:20:00"
    "【Akamai】 asia-vcode-api.vivoglobal.com 【Akamai CDN+】 vivo-告警，域名：asia-vcode-api.vivoglobal.com发生了5xx状态码超出5%25告警，当前值：50%25。最近一次告警时间：2023-08-19 00:40:00"
    "【Akamai】 asia-vcode-api.vivoglobal.com 【Akamai CDN+】 vivo-告警，域名：asia-vcode-api.vivoglobal.com发生了5xx状态码超出5%25告警，当前值：55%25。最近一次告警时间：2023-08-19 01:00:00"
    "【Akamai】 asia-vcode-api.vivoglobal.com 【Akamai CDN+】 vivo-告警，域名：asia-vcode-api.vivoglobal.com发生了5xx状态码超出5%25告警，当前值：55%25。最近一次告警时间：2023-08-19 01:20:00"
    "【Akamai】 asia-vcode-api.vivoglobal.com 【Akamai CDN+】 vivo-告警，域名：asia-vcode-api.vivoglobal.com发生了5xx状态码超出5%25告警，当前值：55%25。最近一次告警时间：2023-08-19 01:40:00"

)

# Function to send SMS with custom message
send_custom_sms() {
    custom_message="$1"

    curl -X POST $SEND_SMS_URL \
         -d "from=$SENDER" \
         -d "to=$RECIPIENT1" \
         -d "text=$custom_message" \
         -d "api_key=$API_KEY" \
         -d "api_secret=$API_SECRET"

    curl -X POST $SEND_SMS_URL \
         -d "from=$SENDER" \
         -d "to=$RECIPIENT2" \
         -d "text=$custom_message" \
         -d "api_key=$API_KEY" \
         -d "api_secret=$API_SECRET"

    curl -X POST $SEND_SMS_URL \
         -d "from=$SENDER" \
         -d "to=$RECIPIENT3" \
         -d "text=$custom_message" \
         -d "api_key=$API_KEY" \
         -d "api_secret=$API_SECRET"

    curl -X POST $SEND_SMS_URL \
         -d "from=$SENDER" \
         -d "to=$RECIPIENT4" \
         -d "text=$custom_message" \
         -d "api_key=$API_KEY" \
         -d "api_secret=$API_SECRET"
}

# Define time points for sending SMS
time_points=("16:00" "16:20" "16:40" "17:00" "17:20" "17:40")

lastIndex=0;
# Infinite loop to check current time and send SMS at the right time
while true; do
    current_time=$(date "+%H:%M")

    for i in "${!time_points[@]}"; do
        if [[ "$current_time" == "${time_points[$i]}" ]]; then
            send_custom_sms "${messages[$((lastIndex%6))]}"
            ((lastIndex++))
            echo ${current_time}
            sleep 60  # Sleep for 1 minute after sending SMS
            break
        fi
    done

    sleep 1  # Sleep for 1 second before checking the time again
done
