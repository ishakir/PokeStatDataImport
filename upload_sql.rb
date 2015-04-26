require "mysql2"
require "json"
require "set"

def generations_and_tiers(client)
	sql =   "SELECT g.number, t.name, t.id FROM tiers AS t\n" + 
			"    JOIN generations AS g\n" + 
			"        ON t.generation_id = g.id\n"

	results = client.query(sql)
	result = Hash.new{|h,k| h[k] = {}}
	results.each do |row|
		result[row["number"]] = Hash.new{|h,k| h[k] = {}}
		result[row["number"]][row["name"]] = row["id"]
	end
	result
end

def years_and_months(client)
	sql =   "SELECT y.number AS year, m.number AS month, m.id FROM months AS m\n" + 
			"	JOIN years as y\n" + 
			"		ON m.year_id = y.id"

	results = client.query(sql)
	result = Hash.new{|h,k| h[k] = {}}
	results.each do |row|
		result[row["year"]] = Hash.new{|h,k| h[k] = {}}
		result[row["year"]][row["month"]] = row["id"]
	end
	result
end

def ev_spreads(client)
	sql =   "SELECT id, hp, attack, defence, spa, spd, speed FROM ev_spreads"

	results = client.query(sql)
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

def update_abilities(data, cache, client)
	new_abilities = Set.new
	data["data"].keys.each do |pokemon|
		data["data"][pokemon]["Abilities"].keys.each do |ability|
			new_abilities.add(ability) if !cache.key?(ability)
		end
	end
	
	new_abilities_sql = new_abilities.to_a.map do |ability|
		"('" + ability.gsub("'", "''") + "')"
	end

	client.query("INSERT INTO abilities (name) VALUES " + new_abilities_sql.join(",")) if !new_abilities_sql.empty?
	abilities(client)
end

def update_pokemon(data, cache, client)
	new_pokemon = Set.new
	data["data"].keys.each do |p|
		new_pokemon.add(p) if !cache.key?(p)
		data["data"][p]["Teammates"].keys.each do |pokemon|
			new_pokemon.add(pokemon) if !cache.key?(pokemon)
		end
		data["data"][p]["Checks and Counters"].keys.each do |pokemon|
			new_pokemon.add(pokemon) if !cache.key?(pokemon)
		end
	end
	
	new_pokemon_sql = new_pokemon.to_a.map do |name| 
		"('" + name.gsub("'", "''") + "')"
	end

	client.query("INSERT INTO pokemon (name) VALUES " + new_pokemon_sql.join(",")) if !new_pokemon_sql.empty?
	pokemon(client)
end

def update_moves(data, cache, client)
	new_moves = Set.new
	data["data"].keys.each do |pokemon|
		data["data"][pokemon]["Moves"].keys.each do |move|
			new_moves.add(move) if !cache.key?(move)
		end
	end

	new_moves_sql = new_moves.to_a.map do |move|
		"('" + move.gsub("'", "''") + "')"
	end

	client.query("INSERT INTO moves (name) VALUES " + new_moves_sql.join(",")) if !new_moves_sql.empty?
	moves(client)
end

def update_items(data, cache, client)
	new_items = Set.new
	data["data"].keys.each do |pokemon|
		data["data"][pokemon]["Items"].keys.each do |item|
			new_items.add(item) if !cache.key?(item)
		end
	end

	new_items_sql = new_items.to_a.map do |item|
		"('" + item.gsub("'", "''") + "')"
	end

	client.query("INSERT INTO items (name) VALUES " + new_items_sql.join(",")) if !new_items_sql.empty?
	items(client)
end

def update_spreads(data, natures_cache, ev_spreads_cache, client)
	regex = /([A-z]+):(\d+)\/(\d+)\/(\d+)\/(\d+)\/(\d+)\/(\d+)/

	new_natures = Set.new
	new_ev_spreads = Set.new

	data["data"].keys.each do |pokemon|
		data["data"][pokemon]["Spreads"].keys.each do |spread|
			match = spread.scan(regex)

			nature = match[0][0]
			ev_spread = {
				hp: match[0][1],
				attack: match[0][2],
				defence: match[0][3],
				spa: match[0][4],
				spd: match[0][5],
				speed: match[0][6]
			}

			new_natures.add(nature) if !natures_cache.key?(nature)
			new_ev_spreads.add(ev_spread) if !ev_spreads_cache.key?(ev_spread)
		end
	end

	new_natures_sql = new_natures.to_a.map do |nature|
		"('" + nature.gsub("'", "''") + "')"
	end

	new_ev_spreads_sql = new_ev_spreads.to_a.map do |e|
		"(" + [e[:hp], e[:attack], e[:defence], e[:spa], e[:spd], e[:speed]].join(",") + ")"
	end

	puts "INSERT INTO natures (name) VALUES " + new_natures_sql.join(",")
	puts "INSERT INTO ev_spreads (hp, attack, defence, spa, spd, speed) VALUES " + new_ev_spreads_sql.join(",")
	[natures(client), ev_spreads(client)]
end

def add_generation_and_tier(cache, generation, tier)
	sql = ""
	sql += "INSERT INTO generations VALUES (%{generation});" % { generation: generation }
	sql += "INSERT INTO tiers VALUES (%{tier});" % { tier: tier }
	execute(sql)
end

def upload_file(year, month, generation, tier, tier_rating, data, leads, client)	
	# Create new mysql connection

	generations_and_tiers = generations_and_tiers(client)   # Generation -> Tier -> tier_id
	months = years_and_months(client)						# Year -> Month -> month_id

	# Upload new stuff and update caches
	abilities_cache = update_abilities(data, abilities(client), client)
	natures_cache, ev_spreads_cache = update_spreads(data, natures(client), ev_spreads(client), client)
	pokemon_cache = update_pokemon(data, pokemon(client), client)
	moves_cache = update_moves(data, moves(client), client)
	items_cache = update_items(data, items(client), client)

	# Grab some important information
	# if !generations_and_tiers.key?(generation)
	# 	generations_and_tiers = add_generation_and_tier(generations_and_tiers, generation, tier)
	# elsif !generations_and_tiers[generation].key?(tier)
	# 	generations_and_tiers = add_tier(generations_and_tiers, generation, tier)
	# end

	# # TODO do the same for dates and months as we did with generations and tiers

	# ## This will be a list of [list of sql statements]
	# pokemon_sql = data["data"].keys.map do |pokemon|
	# 	pokemon_data = data["data"][pokemon]

	# 	sql = []
	# 	sql.append("INSERT INTO stat_records VALUES (#{pokemon_data["Raw count"]}, #{pokemon_id} #{tier_rating_id});")
	# 	sql.append("@stat_record_id = SELECT id FROM stat_records WHERE pokemon_id = 'pokemon_id' AND tier_rating_id = #{tier_rating_id}")

	# 	ability_data = get_ability_data(pokemon_data)
	# 	item_data = get_items_data(pokemon_data)
	# 	move_data = get_move_data(pokemon_data)
	# 	spread_data = get_ev_spreads_data(pokemon_data)
	# 	checks_data = get_checks_data(pokemon_data)
	# 	teammates_data = get_teammates_data(pokemon_data)

	# 	# if leads.key?(pokemon)
	# 	# 	sql.append("INSERT INTO lead_records VALUES (%{number}, @stat_record_id);" % { number: leads[pokemon] })
	# 	# end

	# 	ability_data.each do |ability_record|
	# 		sql.append("IMSERT INTO ability_records VALUES (%{number}, %{ability_id}, @stat_record_id);" % { number: ability_record["number"], ability_id: abilities_cache[ability_record["name"]]})
	# 	end

	# 	item_data.each do |item_record|
	# 		sql.append("INSERT INTO item_records VALUES (%{number}, %{item_id}, @stat_record_id);"  % { number: item_record["number"], item_id: items_cache[item_record["name"]]})
	# 	end

	# 	move_data.each do |move_record|
	# 		sql.append("INSERT INTO move_records VALUES (%{number}, %{move_id}, @stat_record_id);" % { number: move_record["number"], move_id: moves_cache[move_record["name"]] })
	# 	end

	# 	spread_data.each do |spread_record|
	# 		sql.append("INSERT INTO spread_records VALUES (%{number}, %{ev_spread_id}, %{nature_id}, @stat_record_id);" % { number: spread_record["number"], ev_spread_id: ev_spreads_cache[spread_record["ev_spread"]], nature_id: natures_cache[spread_record["nature"]]})
	# 	end

	# 	checks_data.each do |check_record|
	# 		sql.append("INSERT INTO check_records VALUES (%{matchup_occurences}, %{kos_or_switches_caused}, %{kos_or_switches_stddev}, %{pokemon_id}, @stat_record_id);" % { matchup_occurences: check_record["matchup_occurences"], kos_or_switches_caused: check_record["kos_or_switches_caused"], kos_or_switches_stddev: check_record["kos_or_switches_stddev"], pokemon_id: pokemon_cache[check_record["pokemon"]]})
	# 	end

	# 	teammates_data.each do |teammate_record|
	# 		sql.append("INSERT INTO teammate_records VALUES (%{number}, %{pokemon_id}, @stat_record_id);" % { number: teammate_record["number"], pokemon_id: pokemon_cache[teammate_record["pokemon"]]})
	# 	end

	# 	execute(sql)

	# end

end

client = Mysql2::Client.new(host: "pokestat.org.uk", username: "pokestat_dev", password: "pokestat_dev")
client.query("USE pokestat_dev;")
data = JSON.parse(IO.read("1v1-0.json"))
upload_file(2014, 11, 6, "1v1", 0, data, nil, client)

# options = {}
# OptionParser.new do |opts|
#   opts.banner = "Usage: ruby upload.rb --year [year] --month [month]"

#   opts.on("--year YEAR") do |year|
#     options[:year] = year
#   end

#   opts.on("--month MONTH") do |month|
#     options[:month] = month
#   end
# end.parse!

# current_date = Date.new(options[:year].to_i, options[:month].to_i, 01)

# while(true) do
#   year = current_date.year
#   month = current_date.month

#   smogon_base_url = "http://www.smogon.com/stats/#{year}-#{month}/"
#   chaos_url = "#{smogon_base_url}chaos/"
#   leads_url = "#{smogon_base_url}leads/"

#   # Wait for the chaos directory for that year / month combo to become available
#   while(get(chaos_url).code != "200") do 
#     puts "Still haven't found chaos files for #{current_date}"
#     sleep(5 * 60)
#   end

#   directory = "logs/#{year}/#{month}"
#   FileUtils.mkdir_p(directory)

#   chaos_files = get(chaos_url).body.scan(/<a.*>(.*.json)<\/a>/)

#   counter = 1
#   total_files = chaos_files.size

#   chaos_files.each do |filename|
#     match = filename[0].scan(/(?:gen(\d))?(.*)-(\d+).json/)

#     generation = match[0][0] ? match[0][0] : CURRENT_GENERATION
#     tier = match[0][1]
#     rating = match[0][2]

#     data = JSON.parse(get("#{chaos_url}#{filename[0]}").body)
#     as_text_file = filename[0].sub("json", "txt")
#     leads = parse_leads("#{leads_url}#{as_text_file}")

#     append("#{directory}/files.log", "Uploading #{filename[0]} - #{counter}/#{total_files}\n")
#     unless year && month && generation && tier && rating
#       puts "ERROR: Couldn't find data for this file!!"
#       puts filename
#       puts "Year: #{year}"
#       puts "Month: #{month}"
#       puts "Generation: #{generation}"
#       puts "Tier: #{tier}"
#       puts "Rating: #{rating}"
#       next
#     end
#     upload_file(year, month, generation, tier, rating, data, leads)
#     counter += 1
#   end

#   current_date = (current_date >> 1)
# end