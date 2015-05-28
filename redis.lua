local MAX_INT = 4294967294 - 2

local function EXISTS(rec, bin)
	if aerospike:exists(rec)
		and rec[bin] ~= nil
			and type(rec) == "userdata"
				and record.ttl(rec) < (MAX_INT - 60) then
		--info("EXISTS true - "..tostring(bin))
		return true
	end
    --info("EXISTS false - "..tostring(bin))
	return false
end

local function UPDATE(rec)
	if aerospike:exists(rec) then
		aerospike:update(rec)
	else
		aerospike:create(rec)
	end
end

function GET(rec, bin)
	if EXISTS(rec, bin) then
		return rec[bin]
	end
	return nil
end

function DEL(rec, bin)
	if EXISTS(rec, bin) then
		rec[bin] = nil
		UPDATE(rec)
		return 1
	end
	return 0
end

function TTL(rec, bin)
	if EXISTS(rec, bin) then
    return record.ttl(rec)
  end
  return nil
end

function SETNX(rec, bin, value)
	if EXISTS(rec, bin) then
		return 0
	else
		rec[bin] = value
		UPDATE(rec)
		return 1
	end
end

function SETNXEX(rec, bin, value, ttl)
	if EXISTS(rec, bin) then
		return 0
	else
		rec[bin] = value
		UPDATE(rec)
		record.set_ttl(rec, ttl)
		return 1
	end
end


function SET(rec, bin, value)
	rec[bin] = value
	UPDATE(rec)
	return "OK"
end

function SETEX(rec, bin, value, ttl)
	rec[bin] = value
	record.set_ttl(rec, ttl)
	UPDATE(rec)
	return "OK"
end

function EXPIRE(rec, bin, ttl)
	if EXISTS(rec, bin) then
		record.set_ttl(rec, ttl)
		UPDATE(rec)
	end
	return "OK"
end

function LPOP (rec, bin, count)
	if (EXISTS(rec, bin)) then
		local l = rec[bin]
		local new_l = list.drop(l, count)
		rec[bin] = new_l
		UPDATE(rec)
		return list.take(l, count)
	end
	return nil
end

function LPUSH(rec, bin, value)
  local l = rec[bin]
  if (l == nil) then
    l = list()
  end
  list.prepend(l, value)
  rec[bin] = l
  local length = #l
  UPDATE(rec)
  return length
end

function LTRIM (rec, bin, start, stop)
	if (EXISTS(rec, bin)) then
		local l = rec[bin]
		local switch = 0

		if (start < 0) then
			start = #l + start
			switch = switch + 1
		end

		if (stop < 0) then
			stop = #l + stop
			switch = switch + 1
		end

		if ((start > stop) and (switch == 1)) then
			local tmp = stop
			stop = start
			start = tmp
		end

		if (start == stop) then
			local v = l[start + 1]
			local l = list()
			list.prepend(l, v)
			rec[bin] = l
		elseif (start < stop) then
			local pre_list  = list.drop(l, start)
			if pre_list == nil then
			  pre_list = l
			end
			local post_list = list.take(pre_list, stop - start + 1)
			rec[bin] = post_list
		else
			rec[bin] = list()
		end
		UPDATE(rec)
	end
	return "OK"
end

function LSIZE(rec, bin)
	if (EXISTS(rec, bin)) then
		local l     = rec[bin]
		return #l
	end
	return nil
end

function RPOP (rec, bin, count)
	if (EXISTS(rec, bin)) then
		local l     = rec[bin]
 		local result_list = nil
		if (#l <= count) then
			result_list = rec[bin]
			rec[bin] = nil
		else
      local start = #l - count
			result_list = list.drop(l, start)
			rec[bin] = list.take(l, start)
		end
		UPDATE(rec)
		if (result_list ~= nil) then
			return result_list
		else
			return list()
		end
	end
	return nil
end

function RPUSH (rec, bin, value)
	local l = rec[bin]
	if (l == nil) then
		l = list()
	end
	list.append(l, value)
	rec[bin] = l
	local length = #l
	UPDATE(rec)
	return length
end

