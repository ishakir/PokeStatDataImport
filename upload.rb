require 'date'
require 'fileutils'
require 'json'
require 'net/http'
require 'optparse'

API_URL = "http://localhost:9000/api"

ABILITY_URL         = API_URL + "/abilities"
ABILITY_RECORD_URL  = API_URL + "/abilityrecords"
EV_SPREAD_URL       = API_URL + "/evspreads"
CHECK_RECORD_URL    = API_URL + "/checkrecords"
GENERATION_URL      = API_URL + "/generations"
ITEM_URL            = API_URL + "/items"
ITEM_RECORD_URL     = API_URL + "/itemrecords"
LEAD_RECORD_URL     = API_URL + "/leadrecords"
MONTH_URL           = API_URL + "/months"
MOVE_URL            = API_URL + "/moves"
MOVE_RECORD_URL     = API_URL + "/moverecords"
NATURE_URL          = API_URL + "/natures"
POKEMON_URL         = API_URL + "/dbpokemon"
SPREAD_RECORD_URL   = API_URL + "/spreadrecords"
STAT_RECORD_URL     = API_URL + "/statrecords"
TEAMMATE_RECORD_URL = API_URL + "/teammaterecords"
TIER_URL            = API_URL + "/tiers"
TIER_MONTH_URL      = API_URL + "/tiermonths"
TIER_RATING_URL     = API_URL + "/tierratings"
YEAR_URL            = API_URL + "/years"

CURRENT_GENERATION = 6

def replace_whitespace(name)
  name.tr(" ", "+")
end

def stringify(name)
  name.to_s
end

def create(uri, hash)
  req = Net::HTTP::Post.new(uri, initheader = {'Content-Type' => 'application/json'})
  req.body = hash.to_json
  response = Net::HTTP.new(uri.hostname, uri.port).start {
    |http| http.request(req)
  }
  if response.code != "200"
    puts "Error uploading " + hash.to_s + " to " + uri.to_s + " response was " + response.code + " body was " + response.body
  end
  sleep(1.0/100.0)
  response
end

def find(uri)
  req = Net::HTTP::Get.new(uri)
  res = Net::HTTP.new(uri.hostname, uri.port).start { |http|
    http.request(req)
  }
  sleep(1.0/1000.0)
  res
end

def get(url)
  uri = URI(url)
  res = nil
  while res == nil do
    begin
      req = Net::HTTP::Get.new(uri)
      res = Net::HTTP.new(uri.hostname, uri.port).start { |http|
        http.request(req)
      }
    rescue => e
      puts "Error connecting to '#{uri}': #{e}"
      puts "Sleeping 5 seconds and retrying"
      sleep(5)
    end
  end
  sleep(1.0/1000.0)
  res
end

def create_return_id(uri, hash)
  create_response = create(URI(uri), hash)
  create_response_body = JSON.parse(create_response.body)
  create_response_body["data"]["id"]
end

def form_uri_from_hash(uri, value_hash)
  equals_statements = value_hash.map { |(key, value)| key.to_s + "=" + value }
  query_string = equals_statements.join("&")
  URI(uri + "?" + query_string)
end

def remove_whitespace_from_hash_values(hash)
  Hash[hash.map { |key, value| [key, replace_whitespace(value.to_s)] }]
end

def stringify_hash_values(hash)
  Hash[hash.map { |key, value| [key, stringify(value.to_s)]}]
end

def create_if_non_existant_return_id(uri, value_hash)
  find_uri = form_uri_from_hash(uri, remove_whitespace_from_hash_values(value_hash))
  find_response = find(find_uri)
  find_response_body = JSON.parse(find_response.body)
  if find_response_body["data"].length == 1
    find_response_body["data"][0]["id"]
  elsif find_response_body["data"].length == 0
    create_return_id(URI(uri), stringify_hash_values(value_hash))
  else
    puts "Error found more than one value for URI " + find_uri 
  end
end

def parse_ev_spread(spread)
  matches = /\w*:(\d+)\/(\d+)\/(\d+)\/(\d+)\/(\d+)\/(\d+)/.match(spread)
  {
    hp: matches[1],
    attack: matches[2],
    defence: matches[3],
    spa: matches[4],
    spd: matches[5],
    speed: matches[6]
  }
end

def parse_nature(spread)
  /(\w+):.*/.match(spread)[1]
end

def append(filename, data)
  buffer = File.exists?(filename) ? File.size(filename) : 0
  File.write(filename, data.to_s, buffer, mode: 'a')
end

def upload_file(year, month, generation, tier, tier_rating, data, leads)
  logs_directory = "logs/#{year}/#{month}/#{generation}/#{tier}/#{tier_rating}"
  if File.directory?(logs_directory)
    puts "Identified that #{year}-#{month} gen#{generation}#{tier}-#{tier_rating} has already been uploaded"
    return
  end
  FileUtils.mkdir_p(logs_directory)

  stat_record_file = "#{logs_directory}/stat_records"
  move_record_file = "#{logs_directory}/move_records"
  ability_record_file = "#{logs_directory}/ability_records"
  item_record_file = "#{logs_directory}/item_records"
  spread_record_file = "#{logs_directory}/spread_records"
  check_record_file = "#{logs_directory}/check_records"
  teammate_record_file = "#{logs_directory}/teammate_records"
  lead_record_file = "#{logs_directory}/lead_records"

  generation_id  = create_if_non_existant_return_id(GENERATION_URL, {number: generation})
  year_id        = create_if_non_existant_return_id(YEAR_URL, {number: year})
  month_id       = create_if_non_existant_return_id(MONTH_URL, {number: month, year_id: year_id})
  tier_id        = create_if_non_existant_return_id(TIER_URL, {name: tier, generation_id: generation_id})
  tier_month_id  = create_if_non_existant_return_id(TIER_MONTH_URL, {month_id: month_id, tier_id: tier_id})
  tier_rating_id = create_if_non_existant_return_id(TIER_RATING_URL, {rating: tier_rating, tier_month_id: tier_month_id, no_of_battles: data["info"]["number of battles"]})

  number_of_pokemon = data["data"].keys.length
  counter = 0

  data["data"].keys.each do |pokemon|

    counter += 1
    append("#{logs_directory}/pokemon.log", "Uploading #{pokemon} - #{counter}/#{number_of_pokemon}\n")

    pokemon_id = create_if_non_existant_return_id(POKEMON_URL, {name: pokemon})
    stat_record_id = create_return_id(STAT_RECORD_URL, remove_whitespace_from_hash_values({pokemon_id: pokemon_id, tier_rating_id: tier_rating_id, raw_usage: data["data"][pokemon]["Raw count"]}))
    append(stat_record_file, "#{stat_record_id},")

    # Upload moves
    data["data"][pokemon]["Moves"].each do |move, value|
      move_id = create_if_non_existant_return_id(MOVE_URL, {name: move})
      move_record_id = create_return_id(MOVE_RECORD_URL, remove_whitespace_from_hash_values({number: value, stat_record_id: stat_record_id, move_id: move_id}))
      append(move_record_file, "#{move_record_id},")
    end

    # Upload abilities
    data["data"][pokemon]["Abilities"].each do |ability, value|
      ability_id = create_if_non_existant_return_id(ABILITY_URL, {name: ability})
      ability_record_id = create_return_id(ABILITY_RECORD_URL, remove_whitespace_from_hash_values({number: value, stat_record_id: stat_record_id, ability_id: ability_id}))
      append(ability_record_file, "#{ability_record_id},")
    end

    # Upload items
    data["data"][pokemon]["Items"].each do |item, value|
      item_id = create_if_non_existant_return_id(ITEM_URL, {name: item})
      item_record_id = create_return_id(ITEM_RECORD_URL, remove_whitespace_from_hash_values({number: value, stat_record_id: stat_record_id, item_id: item_id}))
      append(item_record_file, "#{item_record_id},")
    end

    # Upload spreads
    data["data"][pokemon]["Spreads"].each do |spread, value|
      ev_spread = parse_ev_spread(spread)
      nature    = parse_nature(spread)

      ev_spread_id = create_if_non_existant_return_id(EV_SPREAD_URL, ev_spread)
      nature_id = create_if_non_existant_return_id(NATURE_URL, {name: nature})

      spread_record_id = create_return_id(SPREAD_RECORD_URL, remove_whitespace_from_hash_values({number: value, ev_spread_id: ev_spread_id, nature_id: nature_id, stat_record_id: stat_record_id}))
      append(spread_record_file, "#{spread_record_id},")
    end

    # Upload checks and counters
    data["data"][pokemon]["Checks and Counters"].each do |check, value|
      matchup_occurences = value[0].to_i
      kos_or_switches_caused = value[1]
      kos_or_switches_stddev = value[2] 

      check_pokemon_id = create_if_non_existant_return_id(POKEMON_URL, {name: check})
      check_record_id = create_return_id(CHECK_RECORD_URL, remove_whitespace_from_hash_values({matchup_occurences: matchup_occurences, kos_or_switches_caused: kos_or_switches_caused, kos_or_switches_stddev: kos_or_switches_stddev, pokemon_id: check_pokemon_id, stat_record_id: stat_record_id}))
      append(check_record_file, "#{check_record_id}")
    end

    data["data"][pokemon]["Teammates"].each do |teammate, value|
      teammate_pokemon_id = create_if_non_existant_return_id(POKEMON_URL, {name: teammate})
      teammate_record_id = create_return_id(TEAMMATE_RECORD_URL, remove_whitespace_from_hash_values({number: value, pokemon_id: teammate_pokemon_id, stat_record_id: stat_record_id}))
      append(teammate_record_file, "#{teammate_record_id},")
    end

    if leads.key?(pokemon)
      lead_record_id = create_return_id(LEAD_RECORD_URL, remove_whitespace_from_hash_values({number: leads[pokemon], stat_record_id: stat_record_id}))
      append(lead_record_file, "#{lead_record_id},")
    end

  end
end

def parse_leads(url)
  response = get(url)
  return {} unless response.code == "200"

  lead_stats = {}
  lines = response.body.split("\n")
  lines.each do |line|
    match = line.scan(/\s*\|\s*\d+\s*\|\s*(\S+)\s*\|\s*(\S+)/)
    next unless match[0]
    lead_stats[match[0][0]] = match[0][1].chomp('%')
  end
  lead_stats
end

options = {}
OptionParser.new do |opts|
  opts.banner = "Usage: ruby upload.rb --year [year] --month [month]"

  opts.on("--year YEAR") do |year|
    options[:year] = year
  end

  opts.on("--month MONTH") do |month|
    options[:month] = month
  end
end.parse!

current_date = Date.new(options[:year].to_i, options[:month].to_i, 01)

while(true) do
  year = current_date.year
  month = current_date.month

  smogon_base_url = "http://www.smogon.com/stats/#{year}-#{month}/"
  chaos_url = "#{smogon_base_url}chaos/"
  leads_url = "#{smogon_base_url}leads/"

  # Wait for the chaos directory for that year / month combo to become available
  while(get(chaos_url).code != "200") do 
    puts "Still haven't found chaos files for #{current_date}"
    sleep(5 * 60)
  end

  directory = "logs/#{year}/#{month}"
  FileUtils.mkdir_p(directory)

  chaos_files = get(chaos_url).body.scan(/<a.*>(.*.json)<\/a>/)

  counter = 1
  total_files = chaos_files.size

  chaos_files.each do |filename|
    match = filename[0].scan(/(?:gen(\d))?(.*)-(\d+).json/)

    generation = match[0][0] ? match[0][0] : CURRENT_GENERATION
    tier = match[0][1]
    rating = match[0][2]

    data = JSON.parse(get("#{chaos_url}#{filename[0]}").body)
    as_text_file = filename[0].sub("json", "txt")
    leads = parse_leads("#{leads_url}#{as_text_file}")

    append("#{directory}/files.log", "Uploading #{filename[0]} - #{counter}/#{total_files}\n")
    unless year && month && generation && tier && rating
      puts "ERROR: Couldn't find data for this file!!"
      puts filename
      puts "Year: #{year}"
      puts "Month: #{month}"
      puts "Generation: #{generation}"
      puts "Tier: #{tier}"
      puts "Rating: #{rating}"
      next
    end
    upload_file(year, month, generation, tier, rating, data, leads)
    counter += 1
  end

  current_date = (current_date >> 1)
end