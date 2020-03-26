require 'redis'
require 'cstore_client'
require 'hiredis'
require 'optparse'
options = {sample_size: 1000}
optparse = OptionParser.new do |opts|
  opts.banner = "Usage: ruby data_points.rb [options]"
  opts.on("--sample-size SIZE", Integer, "Devices to sample from each shard. Default: #{options[:sample_size]}") do |s|
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
client = CStoreClient::Client.new(datacenter: options[:datacenter])
SHARDS = client.connections.length
total_users = client.connections.map { |conn| conn.dbsize }.reduce(:+)
puts "Total Devices: #{total_users}"
data_points = 0
users_sampled = 0
users_with_parcels = 0
timestamp = Time.now.utc.to_i
client.connections.each do |redis|
  keys = redis.pipelined { |r| options[:sample_size].times { r.randomkey } }
  raw_thrifts = redis.pipelined { |r| keys.map { |key| r.get(key) } }
  users = raw_thrifts.map do |thrift|
    begin
      XUser.deserialize(thrift)
    rescue
      STDERR.puts "Error deserializing..."
      XUser::XUser.new
    end
  end
  users_sampled += users.size
  users.each do |user|
    if user.parcel_segments && user.parcel_segments.size > 0
      data_points += user.parcel_segments.size
      users_with_parcels += 1
    end
  end
end
puts "Sampled #{users_sampled} in #{options[:datacenter]} datacenter."
puts "#{users_with_parcels} had some parcels."
puts "#{data_points} parcels were found."
puts "[#{data_points.to_f / users_sampled} per sampled device]"
puts "#{data_points.to_f / users_with_parcels} per device with any parcels"
