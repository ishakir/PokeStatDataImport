require "fileutils"
require "mysql2"
require "json"
require "net/http"
require "optparse"
require "set"

CURRENT_GENERATION = 6

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

def append(filename, data)
  buffer = File.exists?(filename) ? File.size(filename) : 0
  File.write(filename, data.to_s, buffer, mode: 'a')
end

def execute_sql(sql, client, log_file)
	append(log_file, sql + "\n")
	client.query(sql)
end

def upload_tier_rating(year, month, generation, tier, tier_rating, number_of_battles, client, log_path)
	generations = generations(client)
	if(!generations.key?(generation))
		execute_sql("INSERT INTO generations (number) VALUES (#{generation})", client, "#{log_path}/generation_insert.sql")
		generations = generations(client)
	end

	tiers = tiers(client)
	if(!tiers.key?({name: tier, generation_id: generations[generation]}))
		execute_sql("INSERT INTO tiers (name, generation_id) VALUES ('#{tier}',#{generations[generation]})", client, "#{log_path}/tier_insert.sql")
		tiers = tiers(client)
	end

	years = years(client)
	if(!years.key?(year))
		execute_sql("INSERT INTO years (number) VALUES (#{year})", client, "#{log_path}/year_insert.sql")
		years = years(client)
	end

	months = months(client)
	if(!months.key?({number: month, year_id: years[year]}))
		execute_sql("INSERT INTO months (number, year_id) VALUES (#{month},#{years[year]})", client, "#{log_path}/month_insert.sql")
		months = months(client)
	end

	tier_months = tier_months(client)
	tier_id = tiers[{name: tier, generation_id: generations[generation]}]
	month_id = months[{number: month, year_id: years[year]}]
	if(!tier_months.key?({tier_id: tier_id, month_id: month_id}))
		execute_sql("INSERT INTO tier_months (tier_id, month_id) VALUES (#{tier_id},#{month_id})", client, "#{log_path}/tier_month_insert.sql")
		tier_months = tier_months(client)
	end

	tier_ratings = tier_ratings(client)
	tier_month_id = tier_months[{tier_id: tier_id, month_id: month_id}]
	if(!tier_ratings.key?({tier_month_id: tier_month_id, rating: tier_rating}))
		execute_sql("INSERT INTO tier_ratings (tier_month_id, rating, no_of_battles) VALUES (#{tier_month_id}, #{tier_rating}, #{number_of_battles})", client, "#{log_path}/tier_rating_insert.sql")
		tier_ratings = tier_ratings(client)
	end

	tier_ratings[{tier_month_id: tier_month_id, rating: tier_rating}]
end

def ev_spreads(client)
	results = client.query("SELECT id, hp, attack, defence, spa, spd, speed FROM ev_spreads")
	result = Hash.new{|h,k| h[k] = {}}
	results.each do |row|
		result[{
			hp: row["hp"],
			attack: row["attack"],
			defence: row["defence"],
			spa: row["spa"],
			spd: row["spd"],
			speed: row["speed"]
		}] = row["id"]
	end
	result
end

def reverse_id_hash(client, sql, id_label, name_label)
	results = client.query(sql)
	result = {}
	results.each do |row|
		result[row[name_label]] = row[id_label]
	end
	result
end

def abilities(client)
	reverse_id_hash(client, "SELECT name, id FROM abilities", "id", "name")
end

def natures(client)
	reverse_id_hash(client, "SELECT name, id FROM natures", "id", "name")
end

def pokemon(client)
	reverse_id_hash(client, "SELECT name, id FROM pokemon", "id", "name")
end

def moves(client)
	reverse_id_hash(client, "SELECT name, id FROM moves", "id", "name")
end

def items(client)
	reverse_id_hash(client, "SELECT name, id FROM items", "id", "name")
end

def generations(client)
	reverse_id_hash(client, "SELECT number, id FROM generations", "id", "number")
end

def years(client)
	reverse_id_hash(client, "SELECT number, id FROM years", "id", "number")
end

def months(client)
	results = client.query("SELECT id, number, year_id FROM months")
	result = {}
	results.each do |row|
		result[{
			number: row["number"],
			year_id: row["year_id"]
		}] = row["id"]
	end
	result
end

def tiers(client)
	results = client.query("SELECT id, name, generation_id FROM tiers")
	result = {}
	results.each do |row|
		result[{
			name: row["name"],
			generation_id: row["generation_id"]
		}] = row["id"]
	end
	result
end

def tier_months(client)
	results = client.query("SELECT id, tier_id, month_id FROM tier_months")
	result = {}
	results.each do |row|
		result[{
			tier_id: row["tier_id"],
			month_id: row["month_id"]
		}] = row["id"]
	end
	result
end

def tier_ratings(client)
	results = client.query("SELECT id, rating, tier_month_id FROM tier_ratings")
	result = {}
	results.each do |row|
		result[{
			rating: row["rating"],
			tier_month_id: row["tier_month_id"]
		}] = row["id"]
	end
	result
end

def update_abilities(data, cache, client, log_path)
	new_abilities = Set.new
	data["data"].keys.each do |pokemon|
		data["data"][pokemon]["Abilities"].keys.each do |ability|
			new_abilities.add(ability) if !cache.key?(ability)
		end
	end
	
	new_abilities_sql = new_abilities.to_a.map do |ability|
		"('" + ability.gsub("'", "''") + "')"
	end

	execute_sql("INSERT INTO abilities (name) VALUES " + new_abilities_sql.join(","), client, "#{log_path}/ability_inserts.sql") unless new_abilities_sql.empty?
	abilities(client)
end

def update_pokemon(data, leads, cache, client, log_path)
	new_pokemon = Set.new
	data["data"].keys.each do |p|
		new_pokemon.add(p) if !cache.key?(p)
		data["data"][p]["Teammates"].keys.each do |pokemon|
			new_pokemon.add(pokemon) if !cache.key?(pokemon)
		end
		data["data"][p]["Checks and Counters"].keys.each do |pokemon|
			new_pokemon.add(pokemon) if !cache.key?(pokemon)
		end
		leads.keys.each do |pokemon|
			new_pokemon.add(pokemon) if !cache.key?(pokemon)
		end
	end
	
	new_pokemon_sql = new_pokemon.to_a.map do |name| 
		"('" + name.gsub("'", "''") + "')"
	end

	execute_sql("INSERT INTO pokemon (name) VALUES " + new_pokemon_sql.join(","), client, "#{log_path}/pokemon_inserts.sql") unless new_pokemon_sql.empty?
	pokemon(client)
end

def update_moves(data, cache, client, log_path)
	new_moves = Set.new
	data["data"].keys.each do |pokemon|
		data["data"][pokemon]["Moves"].keys.each do |move|
			new_moves.add(move) if !cache.key?(move)
		end
	end

	new_moves_sql = new_moves.to_a.map do |move|
		"('" + move.gsub("'", "''") + "')"
	end

	execute_sql("INSERT INTO moves (name) VALUES " + new_moves_sql.join(","), client, "#{log_path}/move_inserts.sql") unless new_moves_sql.empty?
	moves(client)
end

def update_items(data, cache, client, log_path)
	new_items = Set.new
	data["data"].keys.each do |pokemon|
		data["data"][pokemon]["Items"].keys.each do |item|
			new_items.add(item) if !cache.key?(item)
		end
	end

	new_items_sql = new_items.to_a.map do |item|
		"('" + item.gsub("'", "''") + "')"
	end

	execute_sql("INSERT INTO items (name) VALUES " + new_items_sql.join(","), client, "#{log_path}/item_inserts.sql") unless new_items_sql.empty?
	items(client)
end

def parse_spread(spread)
	regex = /([A-z]+):(\d+)\/(\d+)\/(\d+)\/(\d+)\/(\d+)\/(\d+)/
	match = spread.scan(regex)

	nature = match[0][0]
	ev_spread = {
		hp: match[0][1].to_i,
		attack: match[0][2].to_i,
		defence: match[0][3].to_i,
		spa: match[0][4].to_i,
		spd: match[0][5].to_i,
		speed: match[0][6].to_i
	}

	[nature, ev_spread]
end

def update_spreads(data, natures_cache, ev_spreads_cache, client, log_path)
	new_natures = Set.new
	new_ev_spreads = Set.new

	data["data"].keys.each do |pokemon|
		data["data"][pokemon]["Spreads"].keys do |spread, v|
			nature, ev_spread = parse_spread(spread)
			new_natures.add(nature) if !natures_cache.key?(nature)
			new_ev_spreads.add(ev_spread) if v.to_i > 10 && !ev_spreads_cache.key?(ev_spread)
		end
	end

	new_natures_sql = new_natures.to_a.map do |nature|
		"('" + nature.gsub("'", "''") + "')"
	end

	new_ev_spreads_sql = new_ev_spreads.to_a.map do |e|
		"(" + [e[:hp], e[:attack], e[:defence], e[:spa], e[:spd], e[:speed]].join(",") + ")"
	end

	execute_sql("INSERT INTO natures (name) VALUES " + new_natures_sql.join(","), client, "#{log_path}/nature_inserts.sql") unless new_natures_sql.empty?
	execute_sql("INSERT INTO ev_spreads (hp, attack, defence, spa, spd, speed) VALUES " + new_ev_spreads_sql.join(","), client, "#{log_path}/ev_spread_inserts.sql") unless new_ev_spreads_sql.empty?
	[natures(client), new_ev_spreads_sql.empty? ? ev_spreads_cache : ev_spreads(client)]
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

def upload_file(year, month, generation, tier, tier_rating, data, leads)
	client = Mysql2::Client.new(host: "pokestat.org.uk", username: "pokestat_dev", password: "pokestat_dev")
	client.query("USE pokestat_dev;")

	log_path = "logs/#{year}/#{month}/#{generation}/#{tier}/#{tier_rating}"
	FileUtils.mkdir_p log_path

	# Upload new stuff and update caches
	abilities_cache = update_abilities(data, abilities(client), client, log_path)
	natures_cache, ev_spreads_cache = update_spreads(data, natures(client), ev_spreads(client), client, log_path)
	pokemon_cache = update_pokemon(data, leads, pokemon(client), client, log_path)
	moves_cache = update_moves(data, moves(client), client, log_path)
	items_cache = update_items(data, items(client), client, log_path)

	tier_rating_id = upload_tier_rating(year, month, generation, tier, tier_rating, data["info"]["number of battles"], client, log_path)

	## This will be a list of [list of sql statements]
	data["data"].keys.each do |pokemon|
		pokemon_data = data["data"][pokemon]
		log_file = "#{log_path}/#{pokemon}.sql"
		
		execute_sql("INSERT INTO stat_records (raw_usage, pokemon_id, tier_rating_id) VALUES (#{pokemon_data["Raw count"]}, #{pokemon_cache[pokemon]}, #{tier_rating_id});", client, log_file)
		results = client.query("SELECT id FROM stat_records WHERE pokemon_id = #{pokemon_cache[pokemon]} AND tier_rating_id = #{tier_rating_id}")
		stat_record_id = nil
		results.each do |row|
			stat_record_id = row["id"]
		end

		sql = []
		if leads.key?(pokemon)
			sql.push("INSERT INTO lead_records (number, stat_record_id) VALUES (#{leads[pokemon]}, #{stat_record_id});")
		end

		data["data"][pokemon]["Abilities"].each do |key, value|
			sql.push("INSERT INTO ability_records (number, ability_id, stat_record_id) VALUES (#{value}, #{abilities_cache[key]}, #{stat_record_id});")
		end

		data["data"][pokemon]["Items"].each do |key, value|
			sql.push("INSERT INTO item_records (number, item_id, stat_record_id) VALUES (#{value}, #{items_cache[key]}, #{stat_record_id});")
		end

		data["data"][pokemon]["Moves"].each do |key, value|
			sql.push("INSERT INTO move_records (number, move_id, stat_record_id) VALUES (#{value}, #{moves_cache[key]}, #{stat_record_id});")
		end

		data["data"][pokemon]["Spreads"].each do |key, value|
			nature, ev_spread = parse_spread(key)
			sql.push("INSERT INTO spread_records (number, ev_spread_id, nature_id, stat_record_id) VALUES (#{value}, #{ev_spreads_cache[ev_spread]}, #{natures_cache[nature]}, #{stat_record_id});") if value.to_i > 10
		end

		data["data"][pokemon]["Checks and Counters"].each do |key, value|
			matchup_occurences, kos_or_switches_caused, kos_or_switches_stddev = value
			sql.push("INSERT INTO check_records (matchup_occurences, kos_or_switches_caused, kos_or_switches_stddev, pokemon_id, stat_record_id) VALUES (#{matchup_occurences}, #{kos_or_switches_caused}, #{kos_or_switches_stddev}, #{pokemon_cache[key]}, #{stat_record_id});")
		end

		data["data"][pokemon]["Teammates"].each do |key, value|
			sql.push("INSERT INTO teammate_records (number, pokemon_id, stat_record_id) VALUES (#{value}, #{pokemon_cache[key]}, #{stat_record_id});")
		end

		sql.each do |sql_statement|
			execute_sql(sql_statement, client, log_file)
		end
	end

	client.close()
end

# data = JSON.parse(IO.read("1v1-0.json"))
# leads = {}
# leads["Fletchinder"] = 4.13795
# upload_file(2014, 11, 6, "1v1", 0, data, leads)

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

    generation = match[0][0] ? match[0][0].to_i : CURRENT_GENERATION
    tier = match[0][1]
    rating = match[0][2]

    data = JSON.parse(get("#{chaos_url}#{filename[0]}").body)
    as_text_file = filename[0].sub("json", "txt")
    leads = parse_leads("#{leads_url}#{as_text_file}")

    append("#{directory}/files.log", "#{Time.now.getutc}: Uploading #{filename[0]} - #{counter}/#{total_files}\n")
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
    upload_file(year, month, generation, tier, rating.to_i, data, leads)
    counter += 1
  end

  current_date = (current_date >> 1)
end