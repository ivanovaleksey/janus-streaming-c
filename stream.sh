STREAM_IP=${STREAM_IP:-$(docker-machine ip default)}
echo $STREAM_IP

ffmpeg \
    -f lavfi -re -i "testsrc=duration=-1:size=1280x720:rate=30" \
    -f lavfi -re -i "sine=f=50:beep_factor=6" \
    -pix_fmt yuv420p \
    -c:v libvpx -g 60 -deadline realtime -an -f rtp rtp://$STREAM_IP:5004
    -c:a libopus -vn -f rtp rtp://$STREAM_IP:5002
