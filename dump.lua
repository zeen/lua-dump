--[[

dump.lua

This script attempts to serialize the entirety of the Lua state accessible from within Lua in TSV format.
The serialization is complete enough that it could be loaded in a new Lua session and traversed.
The format is also designed to load sanely into SQL databases.

Usage:

a) dofile("path/to/dump.lua")("path/to/output.tsv")
b) dofile("path/to/dump.lua")(print)
c) dofile("path/to/dump.lua")(file_handle)

The following columns exist in the TSV:

* id:integer - numeric ID assigned to object
* type:string - type
* key_id:integer? - reference to another object
* val_id:integer? - reference to another object
* key_json:string? - JSON serialization of a string, number or boolean
* val_json:string? - JSON serialization of a string, number or boolean

Type strings include both Lua types, and additional associated virtual types:

* table - a table
* kv - a key-value pair in a table
* function - a function
* upvalue - an upvalue for a function
* thread - a coroutine (including the main thread) 
* hook - debug hook associated with a thread
* stackfunc - a function on the thread's stack
* stacklocal - a local value on the stack
* userdata - a userdata object
* uservalue - the Lua value attached to a userdata

Format details:

* ID 0 represents a virtual root table.
* We serialize the global object _G, the registry, value-type metatables, and the main and the current threads as kv of the root.
* Metatables for table and userdata types are referenced in the key_id column.
* tostring() version of non-value types are serialized into val_json.
* Additional meta-data about functions and threads is serialized into key_json.
* key_json is an array for upvalue and stacklocal types.
* nil values lead to empty cells.
* Output is sorted by id.

key_json and val_json may contain certain invalid JSON data when a value cannot be represented in JSON format:

* inf
* -inf
* -nan
* invalid UTF-8 strings

]]

local function dump_state(file, options)
	local print;
	local should_close_file;
	if type(file) == "string" then
		local err;
		file, err = io.open(file, "w+");
		if not file then return file, err; end
		should_close_file = true;
	end
	if io.type(file) == "file" then
		print = function(line)
			file:write(line.."\n");
		end
	elseif type(file) == "function" then
		print = file;
		file = nil;
	else
		return nil, "argument must be filename, file handle or print function";
	end

	local value_types = { string = true, number = true, boolean = true, ["nil"] = true };
	local mt_types = { table = true, userdata = true };
	local kv_types = { table = true, ["function"] = true, thread = true };

	local queue = {};
	local queue_low = 1;
	local queue_high = 1;
	local function push(obj)
		assert(not value_types[type(obj)])
		if obj ~= nil then
			queue[queue_high] = obj;
			queue_high = queue_high + 1;
		end
	end
	local function pop()
		local obj = queue[queue_low];
		if obj ~= nil then
			queue[queue_low] = nil;
			queue_low = queue_low + 1;
		end
		return obj;
	end

	local processed_counter = 0;
	local processed = {};
	local function get_id(obj)
		if nil == obj then return 0; end
		local id = processed[obj];
		if id then return id; end

		push(obj);
		processed_counter = processed_counter + 1;
		processed[obj] = processed_counter;
		return processed_counter;
	end

	local escapes = {
		["\""] = "\\\"", ["\\"] = "\\\\", ["\b"] = "\\b",
		["\f"] = "\\f", ["\n"] = "\\n", ["\r"] = "\\r", ["\t"] = "\\t"};
	for i=0,31 do
		local ch = string.char(i);
		if not escapes[ch] then escapes[ch] = ("\\u%.4X"):format(i); end
	end
	local function simple_json(simple, t)
		if simple == nil then return ""; end
		if t == "number" or t == "boolean" then
			return tostring(simple);
		end
		if t ~= "string" then
			simple = tostring(simple);
		end
		-- FIXME do proper utf-8 and binary data detection
		return "\""..(simple:gsub(".", escapes)).."\"";
	end

	local TSV = {};
	function TSV.header() print("id\ttype\tkey_id\tval_id\tkey_json\tval_json"); end
	function TSV.vertex(...) print(table.concat({...}, "\t")); end
	function TSV.edge(...) print(table.concat({...}, "\t")); end

	local CSV = {};
	local function tsv2csv(s) return s:gsub("\"", "\"\""):gsub("[^\t]+", "\"%1\""):gsub("\t", ","); end
	function CSV.header() print(tsv2csv("id\ttype\tkey_id\tval_id\tkey_json\tval_json")); end
	function CSV.vertex(...) print(tsv2csv(table.concat({...}, "\t"))); end
	function CSV.edge(...) print(tsv2csv(table.concat({...}, "\t"))); end

	local format = options and options.format == "TSV" and TSV or CSV;
	local header = options and options.header and format.header or function() end;
	local vertex = format.vertex;
	local edge = format.edge;

	local root = {
		_G = debug.getregistry()[2];
		registry = debug.getregistry();
		mainthread = debug.getregistry()[1];
		currentthread = coroutine.running();

		nil_mt = debug.getmetatable(nil);
		function_mt = debug.getmetatable(function() end);
		string_mt = debug.getmetatable("");
		boolean_mt = debug.getmetatable(true);
		number_mt = debug.getmetatable(0);
		thread_mt = debug.getmetatable(debug.getregistry()[1]);
	};

	processed[dump_state] = 0;
	processed[root] = 0;

	push(root);

	get_id(root._G);
	get_id(root.registry);
	get_id(root.mainthread);
	get_id(root.currentthread);

	get_id(root.nil_mt);
	get_id(root.function_mt);
	get_id(root.string_mt);
	get_id(root.boolean_mt);
	get_id(root.number_mt);
	get_id(root.thread_mt);

	local GET_UPVALUE_IDS = false;

	header();

	while true do
		local obj = pop();
		if obj == nil then break end

		local id = get_id(obj);
		local t = type(obj);

		if id ~= 0 then
			local mt;
			if t == "table" or t == "userdata" then
				mt = debug.getmetatable(obj);
			end
			local json_key = "";
			if t == "function" then
				local info = debug.getinfo(obj);
				for k,v in pairs(info) do
					if json_key == "" then
						json_key = "{"..simple_json(k)..":"..simple_json(v);
					else
						json_key = json_key..","..simple_json(k)..":"..simple_json(v);
					end
				end
				json_key = json_key.."}";
			elseif t == "thread" then
				json_key = "{\"status\":"..simple_json(coroutine.status(obj)).."}";
			end
			vertex(id, t, mt and get_id(mt) or "", "", json_key, simple_json(obj, t));
		end

		if t == "table" then
			for k,v in pairs(obj) do
				local tk = type(k);
				local tv = type(v);
				local is_simple_key = value_types[tk];
				local is_simple_value = value_types[tv];
				edge(
					id, "kv",
					not(is_simple_key) and get_id(k) or "",
					not(is_simple_value) and get_id(v) or "",
					not(is_simple_key) and "" or simple_json(k, tk),
					not(is_simple_value) and "" or simple_json(v, tv)
				);
			end
		elseif t == "function" then
			for i=1,math.huge do
				local k,v = debug.getupvalue(obj, i);
				if not k then break; end

				local tk = type(k);
				local tv = type(v);
				local is_simple_value = value_types[tv];
				local upvalueid = debug.upvalueid(obj, i);
				edge(
					id, "upvalue",
					GET_UPVALUE_IDS and get_id(upvalueid) or "",
					not(is_simple_value) and get_id(v) or "",
					"["..i..","..simple_json(k, tk).."]",
					not(is_simple_value) and "" or simple_json(v, tv)
				);
			end
			-- TODO param names
		elseif t == "thread" then
			local hookf, hookmask, hookcount = debug.gethook(obj);
			if hookf then
				edge(
					id, "hook",
					get_id(hookf),
					"",
					"",
					"["..simple_json(hookmask)..","..hookcount.."]"
				);
			end
			for i=1,math.huge do
				local info = debug.getinfo(obj, i, "f");
				if not info then break end
				local f_id = get_id(info.func);

				if f_id ~= 0 then
					edge(
						id, "stackfunc",
						f_id,
						"",
						i,
						""
					);

					for j=1,math.huge do
						local k,v = debug.getlocal(obj, i, j);
						if not k then break end

						local tk = type(k);
						local tv = type(v);
						local is_simple_value = value_types[tv];

						edge(
							id, "stacklocal",
							f_id,
							not(is_simple_value) and get_id(v) or "",
							"["..i..","..j..","..simple_json(k, tk).."]",
							not(is_simple_value) and "" or simple_json(v, tv)
						);
					end
				end
			end
		elseif t == "userdata" then
			local v = debug.getuservalue(obj);
			if v ~= nil then
				local tv = type(v);
				local is_simple_value = value_types[tv];
				edge(
					id, "uservalue",
					"",
					not(is_simple_value) and get_id(v) or "",
					"",
					not(is_simple_value) and "" or simple_json(v, tv)
				);
			end
		elseif t ~= "number" and t ~= "string" and t ~= "boolean" then
			error("unknown type: "..t);
		end
	end

	if should_close_file then
		return file:close();
	end
	return true;
end

return dump_state;
