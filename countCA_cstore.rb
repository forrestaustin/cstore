require 'redis'
require 'cstore_client'
require 'hiredis'
require 'optparse'

options = {batch_size: 10}
optparse = OptionParser.new do |opts|
  opts.banner = "Usage: ruby countCA_cstore.rb [options]"
  opts.on("--batch-size SIZE", Integer, "Batch size to sample from each shard. Default: #{options[:sample_size]}") do |s|
    options[:sample_size] = s
  end
  datacenters = %w(east west central sg nl)
  opts.on("-d", "--datacenter DATACENTER", datacenters, "Datacenter to sample (#{datacenters.join(', ')})") do |d|
    options[:datacenter] = d
  end
end
optparse.parse!
unless options[:datacenter]
  puts "missing required argument. Must pass datacenter parameter"
  puts optparse
  exit(1)
end

def inCA(user)
  # P1= [41.996718, -124.209802] P1-P2
  # P2= [41.980386, -120.013025] P3-P4
  # P3= [32.304592, -120.736264]
  # P4= [34.325678, -114.333616]
  begin
    result = 0
    # Will result in 0 if location is nil
    for i in user.locations.length
      lat = user.locations[i].latitude
      lon = user.locations[i].longitude
      if (lat<=42 || lat>= 32.3) && (lon >= -124.2 || lon<= -120.02)
        result += 1
        return result
      end
    end
  rescue
    return 0
  end
end

client = CStoreClient::Client.new(datacenter: options[:datacenter])
SHARDS = client.connections.length
total_users = client.connections.map { |conn| conn.dbsize }.reduce(:+)
puts "Total Devices: #{total_users}"

users_searched = 0
totalCA = 0
client.connections.each do |redis|
  cur = 0
  begin
    cur, keys = redis.scan(cursor=cur, count: 10)
    users_CA = 0
    raw_thrifts = redis.pipelined { |r| keys.map { |key| r.get(key) } }
    raw_thrifts.each do |thrift|
      begin
        user = XUser.deserialize(thrift)
        users_inCA += inCA(user) 
      rescue
        STDERR.puts "Error deserializing..."
        #XUser::XUser.new
      end
    end
    totalCA += users_inCA
    users_searched += 10
  end until cur==0
end

puts "#{users_searched} devices searched"
puts "#{totalCA} devices with CA locations"
puts "#{SHARDS} total number of shards"
