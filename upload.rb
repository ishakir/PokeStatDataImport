require 'json'
require 'net/http'
require 'optparse'

API_URL = "http://localhost:9000/api"

ABILITY_URL     = API_URL + "/abilities"
EV_SPREAD_URL   = API_URL + "/evspreads"
GENERATION_URL  = API_URL + "/generations"
ITEM_URL        = API_URL + "/items"
MONTH_URL       = API_URL + "/months"
MOVE_URL        = API_URL + "/moves"
NATURE_URL      = API_URL + "/natures"
POKEMON_URL     = API_URL + "/dbpokemon"
TIER_URL        = API_URL + "/tiers"
TIER_MONTH_URL  = API_URL + "/tiermonths"
TIER_RATING_URL = API_URL + "/tierratings"
YEAR_URL        = API_URL + "/years"

def replace_whitespace(name)
  name.tr(" ", "+")
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
  sleep(1.0/10.0)
  response
end

def find(uri)
  req = Net::HTTP::Get.new(uri)
  res = Net::HTTP.new(uri.hostname, uri.port).start { |http|
    http.request(req)
  }
  sleep(1.0/10.0)
  res
end

def form_uri_from_hash(uri, value_hash)
  equals_statements = value_hash.map { |(key, value)| key.to_s + "=" + value }
  query_string = equals_statements.join("&")
  URI(uri + "?" + query_string)
end

def create_if_non_existant_return_id(uri, value_hash)
  whitespace_clean_value_hash = Hash[value_hash.map { |key, value| [key, replace_whitespace(value.to_s)] }]
  find_uri = form_uri_from_hash(uri, whitespace_clean_value_hash)
  find_response = find(find_uri)
  find_response_body = JSON.parse(find_response.body)
  if find_response_body["data"].length == 1
    find_response_body["data"][0]["id"]
  elsif find_response_body["data"].length == 0
    create_response = create(URI(uri), whitespace_clean_value_hash)
    create_response_body = JSON.parse(create_response.body)
    create_response_body["data"]["id"]
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

options = {}
OptionParser.new do |opts|
  opts.banner = "Usage: ruby upload.rb --generation [generation] --year [year] --month [month] --tier [tier] --rating-cap [rating-cap]"

  opts.on("--generation GENERATION") do |generation|
    options[:generation] = generation
  end

  opts.on("--year YEAR") do |year|
    options[:year] = year
  end

  opts.on("--month MONTH") do |month|
    options[:month] = month
  end

  opts.on("--tier TIER") do |tier|
    options[:tier] = tier
  end

  opts.on("--rating-cap RATINGCAP") do |rating_cap|
    options[:rating_cap] = rating_cap
  end
end.parse!

file = File.read('data/ou-0.json')
data = JSON.parse(file)

puts options

generation_id  = create_if_non_existant_return_id(GENERATION_URL, {number: options[:generation]})
year_id        = create_if_non_existant_return_id(YEAR_URL, {number: options[:year]})
month_id       = create_if_non_existant_return_id(MONTH_URL, {number: options[:month], year_id: year_id})
tier_id        = create_if_non_existant_return_id(TIER_URL, {name: options[:tier], generation_id: generation_id})
tier_month_id  = create_if_non_existant_return_id(TIER_MONTH_URL, {month_id: month_id, tier_id: tier_id})
tier_rating_id = create_if_non_existant_return_id(TIER_RATING_URL, {rating: options[:rating_cap], tier_month_id: tier_month_id})

number_of_pokemon = data["data"].keys.length
counter = 0

data["data"].keys.each do |pokemon|

  counter += 1
  puts counter.to_s + "/" + number_of_pokemon.to_s

  pokemon_id = create_if_non_existant_return_id(POKEMON_URL, {name: pokemon})

  # Upload moves
  data["data"][pokemon]["Moves"].keys.each do |move|
    move_id = create_if_non_existant_return_id(MOVE_URL, {name: move})
  end

  # Upload abilities
  data["data"][pokemon]["Abilities"].keys.each do |ability|
    ability_id = create_if_non_existant_return_id(ABILITY_URL, {name: ability})
  end

  # Upload items
  data["data"][pokemon]["Items"].keys.each do |item|
    item_id = create_if_non_existant_return_id(ITEM_URL, {name: item})
  end

  # Upload spreads
  data["data"][pokemon]["Spreads"].keys.each do |spread|
    ev_spread = parse_ev_spread(spread)
    nature    = parse_nature(spread)

    ev_spread_id = create_if_non_existant_return_id(EV_SPREAD_URL, ev_spread)
    nature_id = create_if_non_existant_return_id(NATURE_URL, {name: nature})
  end

end