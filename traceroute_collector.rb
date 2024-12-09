#!/usr/bin/env ruby

require 'json'
require 'csv'
require 'net/http'
require 'optparse'
require 'fileutils'
require 'set'
require 'logger'

DEFAULT_TRACEROUTE_URI = 'https://findit.martelogan.workers.dev/trace'
DEFAULT_OUTPUT_DIR = 'results'
DEFAULT_CF_COLO_FILE = 'cf_colos.json'
DEFAULT_TARGETS = [
  { ip: '1.1.1.1', name: 'cf-global-dns', domain: 'one.one.one.one' }
]
DEFAULT_TARGET_IS_GCP = true

BUSINESS_REGIONS = {
  'North America' => 'na',
  'South America' => 'latam',
  'Europe' => 'eu',
  'Africa' => 'afr',
  'Middle East' => 'me',
  'Asia Pacific' => 'apac',
  'Asia' => 'apac',
  'Oceania' => 'apac',
  "Test" => 'test',
}

GCP_REGIONS = {
  "us-west1" => { lat: 45.5946, long: -122.6819, city: "Portland, OR, US" },
  "us-west2" => { lat: 34.0489, long: -118.2529, city: "Los Angeles, CA, US" },
  "us-west3" => { lat: 39.7392, long: -104.9903, city: "Denver, CO, US" },
  "us-west4" => { lat: 40.7608, long: -111.8910, city: "Salt Lake City, UT, US" },
  "us-south1" => { lat: 32.7767, long: -96.7970, city: "Dallas, TX, US" },
  "us-east1" => { lat: 33.8361, long: -81.1637, city: "Moncks Corner, SC, US" },
  "us-east4" => { lat: 39.0438, long: -77.4874, city: "Ashburn, VA, US" },
  "us-east5" => { lat: 36.8508, long: -76.2859, city: "Norfolk, VA, US" },
  "us-central1" => { lat: 41.2586, long: -95.9378, city: "Council Bluffs, IA, US" },
  "southamerica-west1" => { lat: -12.0464, long: -77.0428, city: "Lima, Peru" },
  "southamerica-east1" => { lat: -23.5505, long: -46.6333, city: "São Paulo, Brazil" },
  "northamerica-northeast1" => { lat: 45.5017, long: -73.5673, city: "Montreal, QC, CA" },
  "northamerica-northeast2" => { lat: 43.6532, long: -79.3832, city: "Toronto, ON, CA" },
  "me-west1" => { lat: 31.7683, long: 35.2137, city: "Tel Aviv, Israel" },
  "me-central1" => { lat: 25.276987, long: 55.296249, city: "Dubai, UAE" },
  "me-central2" => { lat: 24.7136, long: 46.6753, city: "Riyadh, Saudi Arabia" },
  "europe-west1" => { lat: 53.3331, long: -6.2489, city: "Dublin, Ireland" },
  "europe-west2" => { lat: 51.5072, long: -0.1276, city: "London, UK" },
  "europe-west3" => { lat: 50.1109, long: 8.6821, city: "Frankfurt, Germany" },
  "europe-west4" => { lat: 48.8566, long: 2.3522, city: "Paris, France" },
  "europe-west6" => { lat: 47.3769, long: 8.5417, city: "Zurich, Switzerland" },
  "europe-west8" => { lat: 53.5511, long: 9.9937, city: "Hamburg, Germany" },
  "europe-west9" => { lat: 52.5200, long: 13.4050, city: "Berlin, Germany" },
  "europe-west10" => { lat: 59.3293, long: 18.0686, city: "Stockholm, Sweden" },
  "europe-west12" => { lat: 41.9028, long: 12.4964, city: "Rome, Italy" },
  "europe-southwest1" => { lat: 40.4168, long: -3.7038, city: "Madrid, Spain" },
  "europe-north1" => { lat: 60.1699, long: 24.9384, city: "Helsinki, Finland" },
  "europe-central2" => { lat: 52.2297, long: 21.0122, city: "Warsaw, Poland" },
  "australia-southeast1" => { lat: -33.8688, long: 151.2093, city: "Sydney, Australia" },
  "australia-southeast2" => { lat: -37.8136, long: 144.9631, city: "Melbourne, Australia" },
  "asia-southeast1" => { lat: 1.3521, long: 103.8198, city: "Singapore, Singapore" },
  "asia-southeast2" => { lat: -6.2088, long: 106.8456, city: "Jakarta, Indonesia" },
  "asia-south1" => { lat: 19.0760, long: 72.8777, city: "Mumbai, India" },
  "asia-south2" => { lat: 12.9716, long: 77.5946, city: "Bangalore, India" },
  "asia-northeast1" => { lat: 35.6895, long: 139.6917, city: "Tokyo, Japan" },
  "asia-northeast2" => { lat: 37.5665, long: 126.9780, city: "Seoul, South Korea" },
  "asia-northeast3" => { lat: 22.3964, long: 114.1095, city: "Hong Kong, Hong Kong" },
  "asia-east1" => { lat: 25.0330, long: 121.5654, city: "Taipei, Taiwan" },
  "asia-east2" => { lat: 23.1291, long: 113.2644, city: "Guangzhou, China" },
  "africa-south1" => { lat: -26.2041, long: 28.0473, city: "Johannesburg, South Africa" }
}

REGION_PRECEDENCE = ['na', 'apac', 'eu', 'latam', 'me', 'afr', 'unknown']
GCP_REGION_PRECEDENCE = [
  /^us-/,           # US regions first
  /^northamerica-/, # Then other NA
  /^asia-/,         # Then Asia
  /^australia-/,    # Then Europe
  /^europe-/,       # Then Australia
  /^southamerica-/, # Then South America
  /^me-/,          # Then Middle East
  /^africa-/        # Then Africa
]

options = {
  traceroute_uri: DEFAULT_TRACEROUTE_URI,
  output_dir: DEFAULT_OUTPUT_DIR,
  cf_colo_file: DEFAULT_CF_COLO_FILE,
  targets: DEFAULT_TARGETS,
  colos: nil,
  region: nil,
  target_is_gcp: DEFAULT_TARGET_IS_GCP,
  verbose: true,
  generate_matrix: true,
  generate_aggregates: true,
  postprocess_only: true,
  keep_sorted: true,
  use_local_json: true,
  retry_count: 1,
}

OptionParser.new do |opts|
  opts.banner = "Usage: traceroute_collector.rb [options]"

  opts.on("--traceroute-uri URI", "Traceroute API URI") do |uri|
    options[:traceroute_uri] = uri
  end
  opts.on("--output-dir DIR", "Output directory for results") do
    |dir| options[:output_dir] = dir
  end
  opts.on("--cf-colo-file FILE", "Cloudflare POP file (JSON or CSV)") do |file|
    options[:cf_colo_file] = file
  end
  opts.on(
    "--targets x,y,z", Array, "Comma-separated list as ip:name:domain, ..."
  ) do |list|
    options[:targets] = list.map do |entry|
      entry_parts = entry.split(":")
      ip = entry_parts.shift
      name = entry_parts.shift  || 'unknown'
      domain = entry_parts.shift || 'unknown'
      { ip: ip, name: name, domain: domain}
    end
  end
  opts.on(
    "--colos x,y,z", Array, "Comma-separated list of Cloudflare colos"
  ) do |list|
    options[:colos] = list
  end
  opts.on("--region REGION", "Short region string (e.g. na, apac)") do |region|
    options[:region] = region
  end
  opts.on("--target-is-gcp", "Enable GCP region lookup for targets") do
    options[:target_is_gcp] = true
  end
  opts.on("--verbose", "Output one row per subcolo in traceroute results") do
    options[:verbose] = true
  end
  opts.on("--generate-matrix", "Generate GCP-to-Colo RTT matrix from existing summary") do
    options[:generate_matrix] = true
  end
  opts.on("--generate-aggregates", "Generate aggregate statistics from existing summary") do
    options[:generate_aggregates] = true
  end
  opts.on("--postprocess-only", "Skip data collection and only generate RTT matrix") do
    options[:postprocess_only] = true
  end
  opts.on("--keep-sorted", "Keep CSV files sorted by region") do
    options[:keep_sorted] = true
  end
  opts.on("--use-local-json", "Use local JSON files for traceroute data") do
    options[:use_local_json] = true
  end
  opts.on("--retry-count COUNT", "Number of retries for traceroute data") do |count|
    options[:retry_count] = count.to_i
  end
end.parse!

if options[:colos] && options[:region]
  raise "Specify either --colos or --region, but not both."
end

def load_colo_data(file_path)
  ext = File.extname(file_path)
  raise "Unsupported file format: #{ext}" unless %w[.json .csv].include?(ext)

  if ext == '.json'
    JSON.parse(File.read(file_path))
  else
    colos = {}
    CSV.foreach(file_path, headers: true) do |row|
      colos[row['colo'].downcase] = {
        name: row['name'],
        region: row['region'],
        city: row['city'],
        country: row['country'],
        lat: row['lat'].to_f.round(2),
        lon: row['lon'].to_f.round(2)
      }
    end
    colos
  end
rescue StandardError => e
  puts "Error loading colo data from #{file_path}: #{e.message}"
  {}
end

def fetch_colos_by_region(colo_data, short_region)
  return colo_data.keys if short_region.nil?
  valid_region = BUSINESS_REGIONS.values.include?(short_region)
  raise "Invalid region: #{short_region}" unless valid_region

  colo_data.select do |_, info|
    BUSINESS_REGIONS[info['region']] == short_region
  end.keys
end

def fetch_geoip_info(ip, skip_anycast: false)
  uri = URI("https://ipinfo.io/#{ip}/json")
  response = Net::HTTP.get(uri)
  data = JSON.parse(response)

  return nil if skip_anycast && data['anycast'] == true

  location = data['loc']&.split(',') || []
  {
    lat: location[0].to_f.round(2) || 'unknown',
    long: location[1].to_f.round(2) || 'unknown',
    country: data['country'] || 'unknown',
    city: "#{data['city']}, #{data['region']}, #{data['country']}" || 'unknown',
    region: data['region'] || 'unknown',
    ip: ip
  }
rescue StandardError
  {
    lat: 'unknown',
    long: 'unknown',
    country: 'unknown',
    city: 'unknown',
    region: 'unknown',
    ip: ip
  }
end

EARTH_RADIUS_KM = 6371
def orthodromic_distance(lat1, lon1, lat2, lon2)
  return 'unknown' if [lat1, lon1, lat2, lon2].include?('unknown')

  d_lat = (lat2.to_f - lat1.to_f) * Math::PI / 180
  d_lon = (lon2.to_f - lon1.to_f) * Math::PI / 180

  a = Math.sin(d_lat / 2)**2 +
      Math.cos(lat1.to_f * Math::PI / 180) *
      Math.cos(lat2.to_f * Math::PI / 180) * Math.sin(d_lon / 2)**2
  c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  (EARTH_RADIUS_KM * c).round(2)
end

def analyze_last_valid_hop(hops, target_ip)
  return [nil, nil] if hops.nil? || hops.empty?

  summary_stats = nil
  geo_info = nil

  hops.reverse_each do |hop|
    hop['nodes']&.each do |node|
      # Skip completely invalid/empty nodes
      next if node['ip'].nil? || node['ip'].empty? ||
              node['name'] == 'NO RESPONSE'

      # Collect summary_stats if we haven't found valid ones yet
      # (including target_ip if it has valid summary_stats)
      if summary_stats.nil? && node['mean_rtt_ms'] != 0
        summary_stats = {
          rtt_ms: (node['mean_rtt_ms'] || 0).to_i,
          min_rtt_ms: (node['min_rtt_ms'] || 0).to_f.round(2),
          max_rtt_ms: (node['max_rtt_ms'] || 0).to_f.round(2),
          std_dev_rtt_ms: (node['std_dev_rtt_ms'] || 0).to_f.round(2),
          ip: node['ip'],
          packet_count: node['packet_count']
        }
      end

      # Collect geo info if we haven't found it yet
      # (skip target_ip for geo lookup)
      if geo_info.nil? && node['ip'] != target_ip
        puts "Checking IP #{node['ip']} for unique location..."
        geo_info = fetch_geoip_info(node['ip'], skip_anycast: true)
      end

      # Break early if we have both pieces of information
      break if summary_stats && geo_info
    end

    # Break early if we have both pieces of information
    break if summary_stats && geo_info
  end

  # Return default geo_info if none found
  geo_info ||= {
    ip: 'unknown',
    city: 'unknown',
    region: 'unknown',
    country: 'unknown',
    lat: 'unknown',
    long: 'unknown'
  }

  summary_stats ||= {
    rtt_ms: nil,
    min_rtt_ms: nil,
    max_rtt_ms: nil,
    std_dev_rtt_ms: nil,
    ip: nil,
    packet_count: nil
  }

  puts "Results indicate path to target via #{geo_info[:city]} in #{summary_stats[:rtt_ms]}ms"

  [summary_stats, geo_info]
end

def map_gcp_region(lat, long)
  return 'not_applicable' if [lat, long].include?('unknown')

  closest_region = nil
  shortest_distance = Float::INFINITY

  GCP_REGIONS.each do |region, coords|
    distance = orthodromic_distance(lat, long, coords[:lat], coords[:long])
    next if distance == 'unknown'

    if distance < shortest_distance
      shortest_distance = distance
      closest_region = region
    end
  end

  closest_region || 'not_applicable'
end

def packet_loss_pct(sent, lost)
  return 0 if sent == 0
  (lost.to_f / sent * 100).round(2)
end

def collect_hop_data(hops)
  return [[], []] if hops.nil? || hops.empty?
  congested_hops = []
  slowest_hops = []

  rtt_values = hops.map do |hop|
    hop.dig('nodes', 0, 'mean_rtt_ms') || 0
  end
  mean_rtt = rtt_values.sum / rtt_values.size
  variance = rtt_values.map { |rtt| (rtt - mean_rtt)**2 }.sum / rtt_values.size
  std_dev_rtt = Math.sqrt(variance)

  hops.each do |hop|
    packets_sent = hop['packets_sent'] || 0
    packets_lost = hop['packets_lost'] || 0
    packet_loss_percent = packet_loss_pct(packets_sent, packets_lost)

    (hop['nodes'] || []).each do |node|
      node_ip = node['ip']
      node_name = node['name']
      mean_rtt_ms = node['mean_rtt_ms'] || 0
      std_dev_rtt_ms = node['std_dev_rtt_ms'] || 0
      min_rtt_ms = node['min_rtt_ms'] || 0
      max_rtt_ms = node['max_rtt_ms'] || 0

      if packet_loss_percent > 50
        congested_hops << {
          ip: node_ip,
          name: node_name,
          packet_loss_percent: packet_loss_percent,
          mean_rtt_ms: mean_rtt_ms.round(2),
          std_dev_rtt_ms: std_dev_rtt_ms.round(2),
          min_rtt_ms: min_rtt_ms.round(2),
          max_rtt_ms: max_rtt_ms.round(2)
        }
      end

      if mean_rtt_ms > mean_rtt + std_dev_rtt
        slowest_hops << {
          ip: node_ip,
          name: node_name,
          mean_rtt_ms: mean_rtt_ms.round(2),
          std_dev_rtt_ms: std_dev_rtt_ms.round(2),
          min_rtt_ms: min_rtt_ms.round(2),
          max_rtt_ms: max_rtt_ms.round(2)
        }
      end
    end
  end

  [congested_hops, slowest_hops]
end

def sort_csv_file(file_path)
  return unless File.exist?(file_path)

  # Read existing CSV data
  rows = []
  headers = nil
  CSV.foreach(file_path, headers: true) do |row|
    headers ||= row.headers
    rows << row.to_h
  end

  return if rows.empty?

  # Sort rows by region precedence
  sorted_rows = rows.sort_by do |row|
    # Convert to string to ensure consistent comparison
    region = row['start_region'] || 'unknown'
    [
      REGION_PRECEDENCE.index(region) || REGION_PRECEDENCE.size,
      region.to_s
    ]
  end

  # Write back sorted data
  CSV.open(file_path, 'w') do |csv|
    csv << headers
    sorted_rows.each { |row| csv << row.values }
  end
end

def append_to_csv(file, data, keep_sorted: true)
  return if data.empty?

  # Sort new data by region precedence
  sorted_data = data.sort_by do |row|
    # Convert to string to ensure consistent comparison
    region = row[:start_region] || row['start_region'] || 'unknown'
    [
      REGION_PRECEDENCE.index(region) || REGION_PRECEDENCE.size,
      region.to_s
    ]
  end

  write_headers = !File.exist?(file)
  CSV.open(file, 'a') do |csv|
    csv << sorted_data.first.keys if write_headers
    sorted_data.each { |row| csv << row.values }
  end

  # Sort entire file if keep_sorted is enabled
  sort_csv_file(file) if keep_sorted
end

def log_skipped_traceroute(
  skipped_data:,
  region_short:,
  colo_name:,
  target:,
  skip_reason:,
  error_details:
)
  puts "Skipping colo=#{colo_name} due to #{skip_reason}"
  skipped_data << {
    start_region: region_short,
    start_colo: colo_name,
    trace_target: target[:name],
    timestamp: Time.now.strftime('%Y-%m-%d %H:%M:%S'),
    target_ip: target[:ip],
    target_domain: target[:domain],
    skipped_reason: skip_reason,
    error_details: error_details
  }
  sleep 10
end

def fetch_traceroute(uri:, colo_name:, target:, region_dir:, options:)
  if options[:use_local_json]
    json_path = File.join(region_dir, "#{target[:name]}_#{target[:ip]}.json")
    if File.exist?(json_path)
      puts "Using local JSON file: #{json_path}"
      return { response: File.read(json_path) }
    end
  end

  puts "Fetching #{colo_name} traceroute to #{target[:ip]} ..."
  puts "URI: #{uri}"
  { response: Net::HTTP.get(URI(uri)) }
rescue StandardError => e
  { error: e.message }
end

def process_traceroute_with_retry(
  colo_info:, uri:, colo_name:, target:, region_dir:, region_short:,
  options:, skipped_data:, csv_data:, suspicious_data:
)
  retries_remaining = options[:retry_count]

  loop do
    result = fetch_traceroute(
      uri: uri, colo_name: colo_name, target: target,
      region_dir: region_dir, options: options
    )

    response = result[:response]
    no_response = response.nil? || response.empty?
    traceroute = JSON.parse(response) unless no_response rescue nil
    no_traceroute = traceroute.nil? || (not traceroute.is_a?(Hash))

    if result[:error] || no_response || no_traceroute || traceroute['result'].nil?
      skip_reason = 'no_traceroute_response'
      error_details = result[:error] || 'not_applicable'
    elsif not traceroute['success']
      skip_reason = 'failed_traceroute_execution'
      error_details = traceroute['error'] || 'not_applicable'
    end

    if skip_reason && retries_remaining > 0
      retries_remaining -= 1
      puts "Retrying due to #{skip_reason}. #{retries_remaining} retries remaining..."
      sleep(5 * (options[:retry_count] - retries_remaining))
      next
    end

    # Log skip if we've exhausted retries
    if skip_reason
      log_skipped_traceroute(
        skipped_data: skipped_data,
        region_short: region_short,
        colo_name: colo_name,
        target: target,
        skip_reason: skip_reason,
        error_details: error_details
      )
      return
    end

    process_successful_traceroute(
      colo_info: colo_info,
      uri: uri,
      traceroute: traceroute,
      region_dir: region_dir,
      region_short: region_short,
      colo_name: colo_name,
      target: target,
      options: options,
      csv_data: csv_data,
      skipped_data: skipped_data,
      suspicious_data: suspicious_data
    )
    return
  end
end

def process_successful_traceroute(
  uri:, colo_info:, traceroute:, region_dir:, region_short:, colo_name:, target:,
  options:, csv_data:, skipped_data:, suspicious_data:
)
  colos_results = traceroute.dig('result', 0, 'colos') || []
  colos_to_process = options[:verbose] ? colos_results : [colos_results[0]]

  json_file = File.join(region_dir, "#{target[:name]}_#{target[:ip]}.json")
  File.write(json_file, JSON.pretty_generate(traceroute))

  # TODO: support multiple traceroutes option for same subcolo
  repeated_subcolos = Set.new
  colos_to_process.each do |colos_data|
    next unless colos_data

    had_error = (not colos_data['error'].nil?)
    if had_error
      log_skipped_traceroute(
        skipped_data: skipped_data,
        region_short: region_short,
        colo_name: colo_name,
        target: target,
        skip_reason: 'traceroute_error',
        error_details: colos_data['error']
      )
      next
    end

    colo_result_meta = colos_data['colo']
    subcolo = colo_result_meta['name'] || 'unknown'
    next if repeated_subcolos.include?(subcolo)

    puts "Processing subcolo=#{subcolo} for colo=#{colo_name}..."

    repeated_subcolos.add(subcolo) # allows 'unknown' subcolo to occur exactly once per top-level colo

    json_file = File.join(region_dir, "#{target[:name]}_#{target[:ip]}_#{subcolo}.json")
    File.write(json_file, JSON.pretty_generate(colos_data))

    target_summary = colos_data['target_summary']
    hops = colos_data['hops'] || []
    traceroute_time_ms = colos_data['traceroute_time_ms'] || 'unknown'

    colo_city_full = colo_result_meta['city']
    if colo_info['city'] && colo_info['city'] != colo_city_full&.split(',').first
      colo_info_name = colo_info['name']
      name_parts = colo_info_name&.split(',') || [colo_info['city']]
      province_code = name_parts.size > 2 ? name_parts[1] : nil
      if province_code.nil? || province_code.empty?
        colo_city_full = "#{colo_info['city']}, #{colo_info['cca2']}" rescue 'unknown'
      else
        colo_city_full = "#{colo_info['city']}, #{province_code}, #{colo_info['cca2']}" rescue 'unknown'
      end
    end
    colo_city_parts = colo_city_full.split(',')
    colo_country_short = colo_city_parts.last.strip if colo_city_parts.size > 1

    puts "Reverse-analyzing #{hops.size} hops from #{colo_city_full} to #{target[:ip]}..."
    summary_stats, final_geo = analyze_last_valid_hop(hops, target[:ip])

    target_distance_km = orthodromic_distance(
      colo_info['lat'], colo_info['lon'], final_geo[:lat], final_geo[:long]
    )

    # Determine if result is suspicious
    colo_country = colo_country_short || colo_info['cca2'] || colo_info['country']
    is_cross_country = final_geo[:country] != 'unknown' &&
                      colo_country != 'unknown' &&
                      final_geo[:country] != colo_country
    is_suspicious = is_cross_country &&
                   summary_stats[:rtt_ms] &&
                   target_distance_km != 'unknown' &&
                   target_distance_km > 1000 &&
                   summary_stats[:rtt_ms] < 5

    if is_suspicious
      puts "Suspicious traceroute encountered & tracked: " \
           "region_short=#{region_short} " \
           "subcolo=#{subcolo} " \
           "colo_country=#{colo_country} " \
           "final_geo_country=#{final_geo[:country]} " \
           "rtt_ms=#{summary_stats[:rtt_ms]}ms " \
           "target_ip=#{target[:ip]} " \
           "summary_stats_ip=#{summary_stats[:ip]} " \
           "final_geo_ip=#{final_geo[:ip]} " \
           "traceroute_uri=#{uri}"

      suspicious_dir = File.join('suspicious', region_dir)
      FileUtils.mkdir_p(suspicious_dir)

      json_base = File.join(region_dir, "#{target[:name]}_#{target[:ip]}")
      suspicious_base = File.join(suspicious_dir, "#{target[:name]}_#{target[:ip]}")

      if File.exist?("#{json_base}.json")
        FileUtils.mv("#{json_base}.json", "#{suspicious_base}.json")
      end

      if File.exist?("#{json_base}_#{subcolo}.json")
        FileUtils.mv("#{json_base}_#{subcolo}.json", "#{suspicious_base}_#{subcolo}.json")
      end

      puts "Moved suspicious traceroute JSON files to #{suspicious_dir}"
    end

    approx_nearest_gcp = options[:target_is_gcp] ? map_gcp_region(
      final_geo[:lat], final_geo[:long]
    ) : 'not_applicable'
    approx_gcp_city = approx_nearest_gcp == 'not_applicable' ? 'not_applicable'
      : GCP_REGIONS[approx_nearest_gcp][:city]

    congested_hops, slowest_hops = collect_hop_data(hops)

    approx_final_hop = (
      final_geo[:city].nil? || final_geo[:city].gsub(/[\s,]+/, '').empty? ? 'unknown' : final_geo[:city]
    )

    row_data = {
      start_region: region_short,
      start_colo: colo_name,
      start_subcolo: subcolo,
      trace_target: target[:name],
      rtt_ms: summary_stats[:rtt_ms] || 0,
      hops_count: hops.size,
      start_city: colo_city_full,
      approx_final_hop: approx_final_hop,
      approx_nearest_gcp: approx_nearest_gcp,
      target_distance_km: target_distance_km,
      approx_gcp_city: approx_gcp_city,
      cross_country: is_cross_country,
      target_ip: target[:ip],
      target_domain: target[:domain],
      stats_ip: summary_stats[:ip],
      final_geo_ip: final_geo[:ip],
      traceroute_time_ms: traceroute_time_ms,
      traceroute_packet_count: summary_stats[:packet_count] || 'unknown',
      min_rtt_ms: summary_stats[:min_rtt_ms] || 0,
      max_rtt_ms: summary_stats[:max_rtt_ms] || 0,
      std_dev_rtt_ms: summary_stats[:std_dev_rtt_ms] || 0,
      colo_lat: colo_info['lat'].to_f.round(2),
      colo_long: colo_info['lon'].to_f.round(2),
      colo_country: colo_country_short || colo_info['cca2'] || colo_info['country'],
      target_lat: final_geo[:lat],
      target_long: final_geo[:long],
      target_country: final_geo[:country],
      congested_hops: congested_hops.to_json,
      slowest_hops: slowest_hops.to_json,
      traceroute_uri: uri
    }

    if is_suspicious
      suspicious_data << row_data
    else
      csv_data << row_data
    end
  end
end

def process_colo(
  colo:, cf_colos:, options:, csv_data:, skipped_data:, suspicious_data:
)
  colo_info = cf_colos[colo.upcase]
  colo_name = colo.downcase
  return unless colo_info

  region_short = BUSINESS_REGIONS[colo_info['region']] || 'unknown'
  region_dir = File.join(options[:output_dir], region_short, colo_name)
  FileUtils.mkdir_p(region_dir)

  options[:targets].each do |target|
    uri = "#{options[:traceroute_uri]}?colos=#{colo_name}&targets=#{target[:ip]}"
    process_traceroute_with_retry(
      colo_info: colo_info,
      uri: uri,
      colo_name: colo_name,
      target: target,
      region_dir: region_dir,
      region_short: region_short,
      options: options,
      csv_data: csv_data,
      skipped_data: skipped_data,
      suspicious_data: suspicious_data
    )
  end
rescue StandardError => e
  error_msg = "#{e.class}: #{e.message}\n#{e.backtrace.join("\n")}"
  puts "Error processing colo #{colo_name}: #{error_msg}"

  options[:targets].each do |target|
    log_skipped_traceroute(
      skipped_data: skipped_data,
      region_short: region_short || 'unknown',
      colo_name: colo_name,
      target: target,
      skip_reason: 'unexpected_error',
      error_details: error_msg
    )
  end
end

# Main execution block
unless options[:postprocess_only]
  Dir.mkdir(options[:output_dir]) unless Dir.exist?(options[:output_dir])
  cf_colos = load_colo_data(options[:cf_colo_file])
  unless options[:colos]
    options[:colos] = fetch_colos_by_region(cf_colos, options[:region])
  end

  csv_data = []
  skipped_data = []
  suspicious_data = []

  options[:colos].each do |colo|
    sleep 3
    process_colo(
      colo: colo,
      cf_colos: cf_colos,
      options: options,
      csv_data: csv_data,
      skipped_data: skipped_data,
      suspicious_data: suspicious_data
    )
  end

  summary_filename = options[:verbose] ? 'traceroute_summary_verbose.csv' : 'traceroute_summary.csv'
  append_to_csv(
    File.join(options[:output_dir], summary_filename),
    csv_data,
    keep_sorted: options[:keep_sorted]
  ) unless csv_data.empty?

  skipped_colos_filename = options[:verbose] ? 'skipped_colos_verbose.csv' : 'skipped_colos.csv'
  append_to_csv(
    File.join(options[:output_dir], skipped_colos_filename),
    skipped_data,
    keep_sorted: options[:keep_sorted]
  ) unless skipped_data.empty?

  suspicious_colos_filename = options[:verbose] ? 'suspicious_colos_verbose.csv' : 'suspicious_colos.csv'
  append_to_csv(
    File.join(options[:output_dir], suspicious_colos_filename),
    suspicious_data,
    keep_sorted: options[:keep_sorted]
  ) unless suspicious_data.empty?
end

def generate_rtt_matrix(summary_file, output_dir, colo_key: 'start_colo', region: nil)
  return unless File.exist?(summary_file)

  matrix_data = {}
  cf_colos = Set.new
  gcp_regions = Set.new
  cf_colo_metadata = {}
  gcp_metadata = {}

  # Read the summary CSV and collect data
  CSV.foreach(summary_file, headers: true) do |row|
    next unless row['approx_nearest_gcp'] != 'not_applicable'

    gcp_region = row['approx_nearest_gcp']
    cf_colo = row[colo_key]
    rtt = row['rtt_ms'].to_f

    matrix_data[gcp_region] ||= {}
    matrix_data[gcp_region][cf_colo] ||= []
    matrix_data[gcp_region][cf_colo] << rtt

    cf_colos.add(cf_colo)
    gcp_regions.add(gcp_region)

    # Collect metadata
    cf_colo_metadata[cf_colo] = {
      city: row['start_city'],
      region: row['start_region']
    }
    gcp_metadata[gcp_region] = {
      city: row['approx_gcp_city']
    }
  end

  # Calculate averages
  matrix = {}
  gcp_regions.each do |gcp|
    matrix[gcp] = {}
    cf_colos.each do |colo|
      rtts = matrix_data.dig(gcp, colo) || []
      matrix[gcp][colo] = rtts.empty? ? nil : (rtts.sum / rtts.size).round(2)
    end
  end

  # Sort CF colos by region precedence and then alphabetically
  sorted_cf_colos = cf_colos.to_a.sort_by do |colo|
    [
      REGION_PRECEDENCE.index(cf_colo_metadata[colo][:region]) || REGION_PRECEDENCE.size,
      colo
    ]
  end

  # Sort GCP regions by precedence pattern matching
  sorted_gcp_regions = gcp_regions.to_a.sort_by do |region|
    [
      GCP_REGION_PRECEDENCE.index { |pattern| region.match?(pattern) } || GCP_REGION_PRECEDENCE.size,
      region
    ]
  end

  region_suffix = region ? "_#{region}" : ""
  colo_type = colo_key == 'start_subcolo' ? 'subcolos' : 'colos'
  matrix_file = File.join(output_dir, "#{colo_type}_rtt_matrix#{region_suffix}.csv")

  CSV.open(matrix_file, 'w') do |csv|
    # Metadata rows
    csv << [''] + [''] + sorted_cf_colos.map { |colo| cf_colo_metadata[colo][:city] }
    csv << [''] + [''] + sorted_cf_colos.map { |colo| cf_colo_metadata[colo][:region] }

    # Header row with CF colos
    csv << [''] + [''] + sorted_cf_colos

    # Data rows
    sorted_gcp_regions.each do |gcp|
      row = [gcp_metadata[gcp][:city], gcp]
      sorted_cf_colos.each do |colo|
        row << (matrix[gcp][colo] || 'N/A')
      end
      csv << row
    end
  end

  puts "RTT matrix generated: #{matrix_file}"
end

def calculate_percentile(values, percentile)
  return nil if values.empty?
  sorted = values.sort
  k = (percentile * (sorted.length - 1)).floor
  sorted[k]
end

def generate_aggregate_stats(summary_file, output_dir, region: nil)
  return unless File.exist?(summary_file)

  # Initialize data structures with all metrics we want to track
  regional_stats = {}
  overall_stats = {
    rtt_ms: [],
    hops_count: [],
    std_dev_rtt_ms: [],
    target_distance_km: [],
    traceroute_time_ms: [],
    min_rtt_ms: [],
    max_rtt_ms: [],
    samples: []
  }

  # Read and collect data
  CSV.foreach(summary_file, headers: true) do |row|
    next if row['rtt_ms'].nil? || row['rtt_ms'].to_f == 0
    next if region && row['start_region'] != region

    current_region = row['start_region']
    regional_stats[current_region] ||= {
      rtt_ms: [],
      hops_count: [],
      std_dev_rtt_ms: [],
      target_distance_km: [],
      traceroute_time_ms: [],
      min_rtt_ms: [],
      max_rtt_ms: [],
      samples: []
    }

    # Store full sample data for finding min/max tuples
    sample = {
      cf_colo: row['start_colo'],
      subcolo: row['start_subcolo'],
      gcp_region: row['approx_nearest_gcp'],
      rtt_ms: row['rtt_ms'].to_f,
      hops_count: row['hops_count'].to_i,
      target_distance_km: row['target_distance_km'].to_f,
      traceroute_time_ms: row['traceroute_time_ms'].to_f,
      min_rtt_ms: row['min_rtt_ms'].to_f,
      max_rtt_ms: row['max_rtt_ms'].to_f,
      std_dev_rtt_ms: row['std_dev_rtt_ms'].to_f
    }

    regional_stats[current_region][:samples] << sample
    overall_stats[:samples] ||= []
    overall_stats[:samples] << sample

    # Collect numeric values for percentile calculations
    %w[rtt_ms hops_count target_distance_km traceroute_time_ms min_rtt_ms max_rtt_ms std_dev_rtt_ms].each do |metric|
      next if row[metric].nil? || row[metric] == 'unknown'
      value = metric == 'hops_count' ? row[metric].to_i : row[metric].to_f
      next if value == 0

      regional_stats[current_region][metric.to_sym] << value
      overall_stats[metric.to_sym] << value
    end
  end

  # Generate aggregate statistics
  aggregate_rows = []

  # Process regional statistics
  regional_stats.each do |region, data|
    aggregate_rows << generate_stat_row(data, region)
  end

  # Process overall statistics
  aggregate_rows << generate_stat_row(overall_stats, 'overall')

  # Write to CSV
  region_suffix = region ? "_#{region}" : ""
  output_file = File.join(output_dir, "traceroute_aggregates#{region_suffix}.csv")

  CSV.open(output_file, 'w') do |csv|
    csv << aggregate_rows.first.keys
    aggregate_rows.each { |row| csv << row.values }
  end

  puts "Aggregate statistics generated: #{output_file}"
end

def generate_stat_row(data, region_name)
  # Define metrics and their order
  primary_metrics = ['rtt_ms', 'hops_count', 'std_dev_rtt_ms']
  secondary_metrics = ['target_distance_km', 'traceroute_time_ms']
  all_metrics = primary_metrics + secondary_metrics

  row = {
    region: region_name,
    sample_size: data[:samples].size
  }

  all_metrics.each do |metric|
    values = data[metric.to_sym]
    next if values.nil? || values.empty?

    case metric
    when 'std_dev_rtt_ms'
      # For std dev, we only take the average
      row["#{metric}_avg"] = (values.sum / values.size).round(2)
    when 'min_rtt_ms'
      # For min_rtt, we take the minimum of minimums
      row["#{metric}"] = values.min.round(2)
    when 'max_rtt_ms'
      # For max_rtt, we take the maximum of maximums
      row["#{metric}"] = values.max.round(2)
    else
      # For other metrics, calculate all statistics
      row["#{metric}_avg"] = (values.sum / values.size).round(2)
      row["#{metric}_min"] = values.min.round(2)
      row["#{metric}_max"] = values.max.round(2)
      row["#{metric}_p50"] = calculate_percentile(values, 0.50).round(2)
      row["#{metric}_p90"] = calculate_percentile(values, 0.90).round(2)

      # Find tuples for min/max values
      min_sample = data[:samples].min_by { |s| s[metric.to_sym] }
      max_sample = data[:samples].max_by { |s| s[metric.to_sym] }

      # Include either subcolo or colo (preferring subcolo) in tuples
      min_tuple = [
        min_sample[:subcolo] || min_sample[:cf_colo],
        min_sample[:gcp_region],
        "#{min_sample[:target_distance_km].round}km"
      ].compact.join(',')

      max_tuple = [
        max_sample[:subcolo] || max_sample[:cf_colo],
        max_sample[:gcp_region],
        "#{max_sample[:target_distance_km].round}km"
      ].compact.join(',')

      row["#{metric}_min_tuple"] = min_tuple
      row["#{metric}_max_tuple"] = max_tuple
    end
  end

  # Ensure consistent column ordering
  ordered_row = { region: row[:region], sample_size: row[:sample_size] }

  # Process each metric in order
  all_metrics.each do |metric|
    if metric == 'std_dev_rtt_ms'
      # Add std_dev right after hops_count
      ordered_row["#{metric}_avg"] = row["#{metric}_avg"]
    else
      # Add columns in specified order
      ['avg', 'min', 'max', 'p50', 'p90'].each do |stat|
        key = "#{metric}_#{stat}"
        ordered_row[key] = row[key] if row.key?(key)
      end

      # Add tuples after all stats
      ordered_row["#{metric}_min_tuple"] = row["#{metric}_min_tuple"] if row.key?("#{metric}_min_tuple")
      ordered_row["#{metric}_max_tuple"] = row["#{metric}_max_tuple"] if row.key?("#{metric}_max_tuple")
    end
  end

  ordered_row
end

# Persist any logs that we printed to console
FileUtils.mkdir_p('logs')
$stdout_logger = Logger.new(
  File.join('logs', 'collector.log'),
  'daily'
)
$stderr_logger = Logger.new(
  File.join('logs', 'collector.err.log'),
  'daily'
)

# Redirect stdout and stderr
$stdout.sync = true
$stderr.sync = true

class MultiIO
  def initialize(*targets)
    @targets = targets
  end

  def write(*args)
    @targets.each { |t| t.write(*args) }
  end

  def close
    @targets.each(&:close)
  end

  def flush
    @targets.each(&:flush)
  end
end

$stdout = MultiIO.new($stdout, $stdout_logger.instance_variable_get(:@logdev).dev)
$stderr = MultiIO.new($stderr, $stderr_logger.instance_variable_get(:@logdev).dev)

summary_filename = options[:verbose] ? 'traceroute_summary_verbose.csv' : 'traceroute_summary.csv'
summary_file = File.join(options[:output_dir], summary_filename)

# Generate standard colo-level matrix
if options[:generate_matrix]
  generate_rtt_matrix(summary_file, options[:output_dir], region: options[:region])
end

# Generate aggregate statistics only if requested
if options[:generate_aggregates]
  generate_aggregate_stats(summary_file, options[:output_dir], region: options[:region])
end

# If using verbose summary, also generate subcolo matrix
if options[:verbose] && options[:generate_matrix]
  generate_rtt_matrix(
    summary_file,
    options[:output_dir],
    colo_key: 'start_subcolo',
    region: options[:region]
  )
end

puts "Data collection complete. Results saved in #{options[:output_dir]}"

# At the end of the script, ensure relevant CSVs are sorted
if options[:keep_sorted]
  Dir.glob(File.join(options[:output_dir], '*.csv')).each do |csv_file|
    # Skip matrix files which have a specific structure
    next if csv_file.include?('rtt_matrix')
    sort_csv_file(csv_file)
  end
end
