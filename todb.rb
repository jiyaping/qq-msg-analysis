#encoding : utf-8

#load libs
['active_record'].each { |lib| require lib }

curr_dir = File.join(File.dirname(__FILE__), "user.db")

#Connection to sqlite3
ActiveRecord::Base.establish_connection(
	:adapter=>"sqlite3",
	:database=>"#{curr_dir}"
)

class Msg < ActiveRecord::Base
end

class AddMsg < ActiveRecord::Migration
	def self.up
		create_table :msgs do |t|
			t.string :id
			t.datetime :time
			t.string :session
			t.string :nick
			t.string :userid
			t.string :content
		end
	end

	def self.down
		drop_table :msgs
	end
end

if not Msg.table_exists?
	AddMsg.up
end

class Token < ActiveRecord::Base
end

class AddToken < ActiveRecord::Migration
	def self.up
		create_table :tokens do |t|
			t.string :id
			t.string :userid
			t.string :sessionid
			t.string :token
			t.string :flag
			t.integer :count
		end
	end

	def self.down
		drop_table :tokens
	end
end

if not Token.table_exists?
	AddToken.up
end

##### generate data list ###
class Nlist < ActiveRecord::Base
end

class AddNlist < ActiveRecord::Migration
	def self.up
		create_table :nlists do |t|
			t.string :id
		end
	end

	def self.down
		drop_table :nlists
	end
end

if not Nlist.table_exists?
	AddNlist.up
	10000.times {n = Nlist.new ; n.save}
end
############################

class MsgParse
	def initialize(file)
		@filepath = file
		@session_node_rule = /={60,}\n消息分组:(.*)\n={60,}\n消息对象:(.*)\n={60,}/
		@msg_node_rule = /(\d{4}-\d{2}-\d{2}\s*\d{1,}:\d{2}:\d{2})\s{0,}\n?(.*?)((\(|<)(.*)(\)|>)){0,1}\n(.*)/
		@count = 0
	end

	def is_session?(str)
		str =~ /={50,}/
	end

	def is_msg?(str)
		str =~ /(\d{4}-\d{2}-\d{2}\s*\d{1,}:\d{2}:\d{2})|(.*)/
	end

	def get_session(str)
		match_group = @session_node_rule.match(str)

		"#{match_group[1]} - #{match_group[2]}" unless match_group.nil?
	end

	def msg2db(str, sessionid)
	  return if sessionid.start_with? '最近联系人'
	  
		match_group = @msg_node_rule.match(str)

		unless  match_group.nil?
			#filter empty msg
			return if match_group[2] =~ /\s*\d{1,}:\d{2}:\d{2}\s*/

			msg = Msg.new
			msg.time = DateTime.strptime(match_group[1], '%Y-%m-%d %H:%M:%S')
			msg.session = sessionid
			msg.nick = match_group[2]
			msg.userid = match_group[5]
			msg.content = match_group[7]
			@count += 1
			if @count % 1000 == 0
				puts @count
			end
			return msg.save
		end
	end

	def get_n_line(file, n)
		lines = []
		n.times {lines << file.gets}
	 
		lines
	end
	
	def parse
		File.open(@filepath, :encoding=>"utf-8") do |file|
	 		curr_block = ""
			curr_node = ""

			while line = file.gets do
				next if line.index('消息记录（此消息记录为文本格式，不支持重新导入')
				if line.index("=" * 60)
					curr_node<< line << get_n_line(file, 4).join("")

					curr_block = get_session(curr_node)
					curr_node = ""
					next
				end

				if line == "\n" and curr_node.delete("\n").size != 0
					msg2db(curr_node, curr_block)
					curr_node = ""
				else
					curr_node << line
				end
			end
		end
	end
end

#file need parse...
msg = MsgParse.new("msg-test.txt")
msg.parse