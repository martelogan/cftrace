#!/usr/bin/env ruby

require 'json'
require 'csv'
require 'net/http'
require 'optparse'
require 'fileutils'

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

options = {
  traceroute_uri: DEFAULT_TRACEROUTE_URI,
  output_dir: DEFAULT_OUTPUT_DIR,
  cf_colo_file: DEFAULT_CF_COLO_FILE,
  targets: DEFAULT_TARGETS,
  colos: nil,
  region: nil,
  target_is_gcp: DEFAULT_TARGET_IS_GCP
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
end.parse!

if options[:colos] && options[:region]
  raise "Specify either --colos or --region, but not both."
elsif options[:colos].nil? && options[:region].nil?
  raise "Specify at least one of --colos or --region."
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

def infer_final_hop_geo(hops, target_ip)
  hops.reverse_each do |hop|
    hop['nodes']&.reverse_each do |node|
      next unless node['ip']

      puts "Checking IP #{node['ip']} for unique location..."
      geo_info = fetch_geoip_info(node['ip'], skip_anycast: true)
      return geo_info if geo_info # Return the first unique location found
    end
  end

  puts "Unable to infer final hop geo. Using default unknown values."
  {
    ip: 'unknown',
    city: 'unknown',
    region: 'unknown',
    country: 'unknown',
    lat: 'unknown',
    long: 'unknown'
  }
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

def append_to_csv(file, data)
  write_headers = !File.exist?(file)
  CSV.open(file, 'a') do |csv|
    csv << data.first.keys if write_headers && data.any?
    data.each { |row| csv << row.values }
  end
end

Dir.mkdir(options[:output_dir]) unless Dir.exist?(options[:output_dir])
cf_colos = load_colo_data(options[:cf_colo_file])
if options[:region]
  options[:colos] = fetch_colos_by_region(cf_colos, options[:region])
end

csv_data = []
skipped_data = []
options[:colos].each do |colo|
  # sleep 20
  colo_info = cf_colos[colo]
  colo_name = colo.downcase
  next unless colo_info

  region_short = BUSINESS_REGIONS[colo_info['region']] || 'unknown'
  region_dir = File.join(options[:output_dir], region_short, colo_name)
  FileUtils.mkdir_p(region_dir)

  options[:targets].each do |target|
    uri = "#{options[:traceroute_uri]}?colos=#{colo_name}&targets=#{target[:ip]}"
    puts "Fetching #{colo_name} traceroute to #{target[:ip]} ..."
    puts "URI: #{uri}"
    response = Net::HTTP.get(URI(uri))

    no_response = response.nil? || response.empty?
    traceroute = JSON.parse(response) unless no_response rescue nil
    if no_response || traceroute.nil? || traceroute['result'].nil?
      puts "Skipping colo=#{colo_name} due to no traceroute response"
      skipped_data << {
        start_region: region_short,
        start_colo: colo_name,
        trace_target: target[:name],
        target_ip: target[:ip],
        target_domain: target[:domain],
        skipped_reason: 'no_traceroute_response'
      }
      sleep 2
      next
    end

    colos_data = traceroute.dig('result', 0, 'colos', 0)
    target_summary = colos_data['target_summary']
    hops = colos_data['hops']
    traceroute_time_ms = colos_data['traceroute_time_ms'] || 'unknown'

    colo_result_meta = colos_data['colo']
    subcolo = colo_result_meta['name'] || 'unknown'
    colo_city_full = colo_result_meta['city'] || colo_info['city'] || 'unknown'
    colo_city_parts = colo_city_full.split(',')
    colo_country_short = colo_city_parts.last.strip if colo_city_parts.size > 1

    final_geo = infer_final_hop_geo(hops, target[:ip])
    approx_nearest_gcp = options[:target_is_gcp] ? map_gcp_region(
      final_geo[:lat], final_geo[:long]
    ) : 'not_applicable'
    approx_gcp_city = approx_nearest_gcp == 'not_applicable' ? 'not_applicable'
      : GCP_REGIONS[approx_nearest_gcp][:city]

    json_file = File.join(region_dir, "#{target[:name]}_#{target[:ip]}.json")
    File.write(json_file, JSON.pretty_generate(traceroute))

    congested_hops, slowest_hops = collect_hop_data(hops)

    csv_data << {
      start_region: region_short,
      start_colo: colo_name,
      trace_target: target[:name],
      rtt_ms: (target_summary['mean_rtt_ms'] || 0).to_i,
      hops_count: hops.size,
      start_city: colo_city_full,
      approx_final_hop: final_geo[:city],
      approx_nearest_gcp: approx_nearest_gcp,
      target_distance_km: orthodromic_distance(
        colo_info['lat'], colo_info['lon'], final_geo[:lat], final_geo[:long]
      ),
      approx_gcp_city: approx_gcp_city,
      start_subcolo: subcolo,
      target_ip: target[:ip],
      target_domain: target[:domain],
      traceroute_time_ms: traceroute_time_ms,
      traceroute_packet_count: target_summary['packet_count'] || 'unknown',
      min_rtt_ms: (target_summary['min_rtt_ms'] || 0).to_f.round(2),
      max_rtt_ms: (target_summary['max_rtt_ms'] || 0).to_f.round(2),
      std_dev_rtt_ms: (target_summary['std_dev_rtt_ms'] || 0).to_f.round(2),
      colo_lat: colo_info['lat'].to_f.round(2),
      colo_long: colo_info['lon'].to_f.round(2),
      colo_country: colo_country_short || colo_info['country'],
      target_lat: final_geo[:lat],
      target_long: final_geo[:long],
      target_country: final_geo[:country],
      congested_hops: congested_hops.to_json,
      slowest_hops: slowest_hops.to_json
    }
  end
end

append_to_csv(
  File.join(options[:output_dir], 'traceroute_summary.csv'), csv_data
)
append_to_csv(
  File.join(options[:output_dir], 'skipped_colos.csv'), skipped_data
)

puts "Data collection complete. Results saved in #{options[:output_dir]}"
