require 'net/http'
require 'uri'
require 'openssl'
require 'benchmark'

URL = "https://34.49.121.93/internal-echo"
SAMPLES = 5

def make_request(uri, use_ssl: true, keep_alive: false)
  Net::HTTP.start(uri.host, uri.port, use_ssl: use_ssl, verify_mode: OpenSSL::SSL::VERIFY_NONE) do |http|
    request = Net::HTTP::Get.new(uri)
    request['Connection'] = 'keep-alive' if keep_alive
    times = []

    SAMPLES.times do |i|
      time = Benchmark.realtime {
        response = http.request(request)
      }
      times << time
      puts "Iteration #{i+1}: #{(time * 1000).round(3)} ms"
    end

    times
  end
end

def log_results(title, times)
  Dir.mkdir('logs') unless Dir.exist?('logs')

  File.open("logs/#{title.downcase.gsub(' ', '_')}.log", 'w') do |file|
    file.puts "#{title} - Timing Measurements"
    times.each_with_index { |time, i| file.puts "Iteration #{i+1}: #{(time * 1000).round(3)} ms" }
    avg_time = times.sum / times.size
    file.puts "Average: #{(avg_time * 1000).round(3)} ms"
  end
end

# Construct URI object **only once**
uri = URI(URL)

puts "\nTesting: Cold TCP & Cold TLS"
cold_tcp_tls_times = make_request(uri)
log_results("Cold TCP & Cold TLS", cold_tcp_tls_times)

# can't easily force cold tls via Ruby request alone...switched to Python sockets for this
# puts "\nTesting: Warm TCP & Cold TLS"
# warm_tcp_tls_times = make_request(uri, keep_alive: true)
# log_results("Warm TCP & Cold TLS", warm_tcp_tls_times)

puts "\nTesting: Warm TCP & Warm TLS"
warm_tcp_warm_tls_times = make_request(uri, keep_alive: true)
log_results("Warm TCP & Warm TLS", warm_tcp_warm_tls_times)

puts "\nSee results in logs/cold_tcp_cold_tls.log, and logs/warm_tcp_warm_tls.log."
