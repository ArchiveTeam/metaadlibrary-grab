local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")
local html_entities = require("htmlEntities")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false
local is_initial_url = true

local item_definitions = {
  ["^https?://[^/]*facebook%.com/ads/library/%?id=([0-9]+)$"]="ad",
  ["^https?://[^/]*facebook%.com/ads/library/.*[%?&]view_all_page_id=([0-9]+)"]="page"
}

cjson.encode_empty_table_as_object(false)

math.randomseed(os.time())

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
print("discovered", item)
    target[item] = true
    return true
  end
  return false
end

find_item = function(url)
  if ids[url] then
    return nil
  end
  local value = nil
  local type_ = nil
  for pattern, name in pairs(item_definitions) do
    value = string.match(url, pattern)
    type_ = name
    if value then
      break
    end
  end
  if value and type_ then
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    local newcontext = {
      ["graphql_countries"]={},
      ["graphql_countries_i"]=0,
      ["initial_url"]=url
    }
    new_item_type = found["type"]
    new_item_value = found["value"]
    new_item_name = new_item_type .. ":" .. new_item_value
    local post_id = string.match(new_item_value, ":([^:]+)$")
    if new_item_name ~= item_name
      and not ids[post_id] then
      ids = {}
      if new_item_type == "page" then
        local country = string.match(url, "&country=([A-Z]+)")
        if not country then
          country = {}
        else
          country = {country}
        end
        newcontext["country"] = country
      end
      context = newcontext
      item_value = new_item_value
      item_type = new_item_type
      ids[string.lower(item_value)] = true
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      item_name = new_item_name
      print("Archiving item " .. item_name)
    end
  end
end

percent_encode_url = function(url)
  temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  return temp
end

allowed = function(url, parenturl)
  local noscheme = string.match(url, "^https?://(.*)$")

  if ids[url]
    or (noscheme and ids[string.lower(noscheme)]) then
    return true
  end

  if string.match(url, "^https?://[^/]*fbcdn%.net/")
    or url == "https://www.facebook.com/api/graphql/" then
    return true
  end

  local skip = false
  for pattern, type_ in pairs(item_definitions) do
    match = string.match(url, pattern)
    if match then
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name then
        discover_item(discovered_items, new_item)
        skip = true
      end
    end
  end
  if skip then
    return false
  end

  if not string.match(url, "^https?://[^/]*facebook%.com/") then
    discover_item(discovered_outlinks, string.match(percent_encode_url(url), "^([^%s]+)"))
    return false
  end

  for _, pattern in pairs({
    "([0-9]+)"
  }) do
    for s in string.gmatch(url, pattern) do
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if allowed(url, parent["url"])
    and not processed(url)
    and string.match(url, "^https://")
    and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end]]

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil
  local post_data = nil
  local post_headers = nil

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    if not string.match(newurl, "^https?://") then
      return nil
    end
    local post_body = nil
    local post_url = nil
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0
      or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if (not processed(url_) or url == "https://www.facebook.com/api/graphql/")
      and allowed(url_, origurl) then
      local headers = {}
      if url == "https://www.facebook.com/api/graphql/" then
        if not post_headers or not post_data then
          return nil
        end
        for k, v in pairs(post_headers) do
          if v == "TODO" then
            error("Found placeholder in HTTP headers.")
          end
          headers[k] = v
        end
      end
      if post_data then
        local body_data = nil
        if type(post_data) == "table" then
          body_data = ""
          for k, v in pairs(post_data) do
            if string.len(body_data) > 0 then
              body_data = body_data .. "&"
            end
            if type(v) ~= "string" then
              error("Found body data value that is not a string.")
            end
            if v == "TODO" then
              error("Found placeholder in body data.")
            end
            body_data = body_data .. k .. "=" .. urlparse.escape(v)
          end
        else
          body_data = post_data
        end
        if type(body_data) ~= "string" then
          error("Body data could not be made into a string.")
        end
        table.insert(urls, {
          url=url_,
          headers=headers,
          body_data=body_data,
          method="POST"
        })
      else
        table.insert(urls, {
          url=url_,
          headers=headers
        })
      end
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function get_count(data)
    local count = 0
    for _ in pairs(data) do
      count = count + 1
    end 
    return count
  end

  local function flatten_json(json)
    local result = ""
    for k, v in pairs(json) do
      result = result .. " " .. k
      local type_v = type(v)
      if type_v == "string" then
        v = string.gsub(v, "\\", "")
        result = result .. " " .. v .. ' "' .. v .. '"'
      elseif type_v == "table" then
        result = result .. " " .. flatten_json(v)
      end
    end
    return result
  end

  local function find_ad_data(json)
    local result = {}
    for k, v in pairs(json) do
      if k == "otherProps" and v["deeplinkAdID"] then
        table.insert(result, v)
      end
      if type(v) == "table" then
        for _, d in pairs(find_ad_data(v)) do
          table.insert(result, d)
        end
      end
    end
    return result
  end

  local function find_json_data(json, f)
    local results = {}
    if type(json) ~= "table" then
      return results
    end
    for k, v in pairs(json) do
      r = f(k, v)
      if r then
        table.insert(results, r)
      else
        for _, r in pairs(find_json_data(v, f)) do
          table.insert(results, r)
        end
      end
    end
    return results
  end

  local function extract_strange_list(json, name, key, extra)
    if not extra then
      extra = function(k, v) return true end
    end
    return find_json_data(
      json,
      function(k, v)
        if type(v) == "table"
          and get_count(v) > 2
          and v[1] == name
          and extra(k, v) then
          return v[3][key]
        end
        return nil
      end
    )
  end

  local function random_string(chars, i)
    local result = ""
    for j = 1, i do
      local num = math.random(string.len(chars))
      result = result .. string.sub(chars, num, num)
    end
    return result
  end

  local function copy_table(data)
    local copy = {}
    for k, v in pairs(data) do
      copy[k] = v
    end
    return copy
  end

  --local int_to_base36(i)

  local function index_graphql_data(json)
    if string.match(url, "^https?://[^/]*facebook%.com/ads/library/") then
      local lsd = extract_strange_list(json, "LSD", "token")[1]
      local lsd_num = 0
      for c in string.gmatch(lsd, "(.)") do
        lsd_num = lsd_num + string.byte(c)
      end
      local connection_class = extract_strange_list(json, "WebConnectionClassServerGuess", "connectionClass")[1]
      local jazoest = extract_strange_list(
            json, "SprinkleConfig", "version",
            function(k, v) return v[3]["param_name"] == "jazoest" end
      )[1]
      local site_data = find_json_data(
        json,
        function(k, v) if k == "SiteData" then return v end return nil end
      )[1]
      local variables = find_json_data(
        json,
        function(k, v) if k == "entryPointParams" then return v end return nil end
      )[1]
      --variables["countries"] = {variables["country"]}
      variables["countries"] = "TODO"
      variables["country"] = nil
      variables["fetchPageInfo"] = nil
      variables["fetchSharedDisclaimers"] = nil
      variables["first"] = 30
      context["graphql_req"] = 0
      context["graphql_variables"] = variables
      context["graphql_headers"] = {
        ["X-ASBD-ID"]=random_string("0123456789", 6),
        ["X-FB-LSD"]=lsd,
        ["X-FB-Friendly-Name"]="TODO",
        ["Referer"]=nil--url
      }
      context["graphql_data"] = {
        ["av"]="0",
        ["__aaid"]="0",
        ["__user"]="0",
        ["__a"]="1",
        ["__req"]="TODO",
        ["__hs"]=site_data["haste_session"],
        ["dpr"]="1",
        ["__ccg"]=connection_class,
        ["__rev"]=tostring(site_data["server_revision"]),
        ["__s"]=(
          random_string("abcdefghijklmnopqrstuvwxyz0123456789", 6)
          .. ":" ..
          random_string("abcdefghijklmnopqrstuvwxyz0123456789", 6)
          .. ":" ..
          random_string("abcdefghijklmnopqrstuvwxyz0123456789", 6)
        ),
        ["__hsi"]=site_data["hsi"],
        -- next four are just for keeping track what is loaded, ~static
        ["__dyn"]="7xeUmwlECdwn8K2Wmh0no6u5U4e1Fx-ewSAwHwNw9G2S2q0_EtxG4o0B-qbwgE1EEb87C1xwEwgo9oO0n24oaEd86a3a1YwBgao6C0Mo6i588Etw8WfK1LwPxe2GewbCXwJwmEtwse5o4q0HU1IEGdw46wbLwrU6C2-0VE6O1Fw59G2O1TwmUaE2Two8",
        ["__csr"]="hkIr9pfuiuW8J8x7888Ln9FKGJ2XBRyXAByrAQ8GHKq6Egx2ECi6bwSBG3m1hwyxy6Uy9xC3S1WwgHwQxS78669Bxa0y87qfxC2y1dzU3TBxO2K0alwm8e8-226Ef9Uhw9K16wk83rx603ROFo017vE08rk00-p80cCU0bSUKdw1j-0fzw72w",
         ["__hsdp"]="gi8mg4ehuBh4UW1owji3EaqGgE450DCwrpF8KBzk1bwiU0A21VU4h0to6u7E9E8aihNDJ3EK1CgfE5608fwDwv80JGt07iy81G825w4HyU887-1dw1Gq0uO015mw1ny05vo",
        ["__hblp"]="02Po0lVw1760dkw4pBw2hE0gbw2Ao0Ki04L80j-w1VG0fnw6ww1nS0OocE2Sw2lo0YK1fwho09-oW2u0YEdo1Uo5u1Tw",
        ["__comet_req"]="1",
        ["lsd"]=lsd,
        ["jazoest"]=tostring(jazoest) .. tostring(lsd_num),
        ["__spin_r"]=tostring(site_data["__spin_r"]),
        ["__spin_b"]=site_data["__spin_b"],
        ["__spin_t"]=tostring(site_data["__spin_t"]),
        ["__jssesw"]="1",
        ["fb_api_caller_class"]="RelayModern",
        ["fb_api_req_friendly_name"]="TODO",
        ["variables"]="TODO",
        ["server_timestamps"]="true",
        ["doc_id"]="TODO"
    }
    elseif not context["graphql_data"] or not context["graphql_headers"] then
      error("No GraphQL data was found.")
    end
  end

  local countries_count = get_count(context["graphql_countries"])

  local function make_graphql_request(name, variables)
    context["graphql_req"] = context["graphql_req"] + 1
    post_data = copy_table(context["graphql_data"])
    post_data["fb_api_req_friendly_name"] = name
    post_data["doc_id"] = ({
      ["AdLibrarySearchPaginationQuery"]="24394279933540792",
      ["AdLibraryPageHoverCardQuery"]="29261964740117378",
    })[name]
    table.insert(context["graphql_countries"], countries_count+1, (variables["countries"] or ""))
    post_data["__req"] = tostring(context["graphql_req"])
    post_data["variables"] = cjson.encode(variables)
    post_headers = copy_table(context["graphql_headers"])
    post_headers["X-FB-Friendly-Name"] = name
--print(cjson.encode(post_headers))
--print(cjson.encode(post_data))
    check("https://www.facebook.com/api/graphql/")
    post_data = nil
    post_headers = nil
  end

  local function extract_all_ads(json)
    local found = 0
    for k, v in pairs(json) do
      if k == "ad_archive_id"
        or k == "deeplinkAdID" then
        if type(v) == "string"
          and v ~= item_value then
          found = found + 1
          discover_item(discovered_items, "ad:" .. v)
        end
        local page_id = json["page_id"] or json["viewAllPageID"]
        if page_id and page_id ~= cjson.null and type(page_id) == "string" then
          discover_item(discovered_items, "page:" .. page_id .. ":")
        end
      elseif type(v) == "table" then
        found = found + extract_all_ads(v)
      end
    end
    return found
  end

  local function check_blocked(s)
    if not s then
      local body, code, headers, status = http.request(context["initial_url"])
      s = body
    end
    for _, key in pairs({"description", "summary"}) do
      local text = string.match(s, "\"" .. key .. "\"%s*:%s*\"([^\"]+)")
      if text
        and string.match(string.lower(text), "blocked") then
        print("Blocked: \"" .. text .. "\".")
        io.stdout:flush()
        wget.callbacks.finish()
        error()
      end
    end
  end

  if allowed(url)
    and status_code < 300
    and not string.match(url, "^https?://[^/]*fbcdn%.net/") then
    html = read_file(file)
    if string.match(url, "^https?://[^/]*facebook%.com/ads/library/") then
      context["html_json"] = {}
      for s in string.gmatch(html, "<script [^>]+data%-sjs>({.-})</script>") do
        table.insert(context["html_json"], cjson.decode(s))
      end
      check_blocked(html)
      local found = extract_all_ads(context["html_json"])
      if string.match(url, "%?id=[0-9]+$") then
        local ad_data = find_ad_data(context["html_json"])
        if get_count(ad_data) ~= 1 then
          error("Expected to find one ad URL for " .. item_value .. ".")
        end
        local inner_data = ad_data[1]["deeplinkAdCard"]["snapshot"]
        if not string.match(cjson.encode(inner_data["images"]), "scontent[^\"/]*%.fbcdn%.net")
          and not string.match(cjson.encode(inner_data["videos"]), "video[^\"/]*%.fbcdn%.net")
          and not string.match(cjson.encode(inner_data["cards"]), "scontent[^\"/]*%.fbcdn%.net")
          and not string.match(cjson.encode(inner_data["cards"]), "video[^\"/]*%.fbcdn%.net") then
          error("Could not find image or video data.")
        end
        html = flatten_json(ad_data)
      end
      if item_type == "page" then
        if found == 0 then
          wget.callbacks.finish()
          print("Error! Sleeping 10 seconds.")
          io.stdout:flush()
          os.execute("sleep 10")
          error()
        end
        index_graphql_data(context["html_json"])
        make_graphql_request(
          "AdLibraryPageHoverCardQuery",
          {["pageID"]=item_value}
        )
        -- skipped SY SD KP IR CU
        --for _, country in pairs({"AD", "AE", "AF", "AG", "AI", "AL", "AM", "AN", "AO", "AQ", "AR", "AS", "AT", "AU", "AW", "AX", "AZ", "BA", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BL", "BM", "BN", "BO", "BQ", "BR", "BS", "BT", "BV", "BW", "BY", "BZ", "CA", "CC", "CD", "CF", "CG", "CH", "CI", "CK", "CL", "CM", "CN", "CO", "CR", "CU", "CV", "CW", "CX", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "DZ", "EC", "EE", "EG", "EH", "ER", "ES", "ET", "FI", "FJ", "FK", "FM", "FO", "FR", "GA", "GB", "GD", "GE", "GF", "GG", "GH", "GI", "GL", "GM", "GN", "GP", "GQ", "GR", "GS", "GT", "GU", "GW", "GY", "HK", "HM", "HN", "HR", "HT", "HU", "ID", "IE", "IL", "IM", "IN", "IO", "IQ", "IS", "IT", "JE", "JM", "JO", "JP", "KE", "KG", "KH", "KI", "KM", "KN", "KR", "KW", "KY", "KZ", "LA", "LB", "LC", "LI", "LK", "LR", "LS", "LT", "LU", "LV", "LY", "MA", "MC", "MD", "ME", "MF", "MG", "MH", "MK", "ML", "MM", "MN", "MO", "MP", "MQ", "MR", "MS", "MT", "MU", "MV", "MW", "MX", "MY", "MZ", "NA", "NC", "NE", "NF", "NG", "NI", "NL", "NO", "NP", "NR", "NU", "NZ", "OM", "PA", "PE", "PF", "PG", "PH", "PK", "PL", "PM", "PN", "PR", "PS", "PT", "PW", "PY", "QA", "RE", "RO", "RS", "RU", "RW", "SA", "SB", "SC", "SE", "SG", "SH", "SI", "SJ", "SK", "SL", "SM", "SN", "SO", "SR", "SS", "ST", "SV", "SX", "SZ", "TC", "TD", "TF", "TG", "TH", "TJ", "TK", "TL", "TM", "TN", "TO", "TR", "TT", "TV", "TW", "TZ", "UA", "UG", "UM", "US", "UY", "UZ", "VA", "VC", "VE", "VG", "VI", "VN", "VU", "WF", "WS", "XK", "YE", "YT", "ZA", "ZM", "ZW"}) do
          local variables = copy_table(context["graphql_variables"])
--          variables["countries"] = {country}
        variables["countries"] = context["country"]
          make_graphql_request(
            "AdLibrarySearchPaginationQuery",
            variables
          )
        --end
        return urls
      end
    end
    if url == "https://www.facebook.com/api/graphql/" then
      context["graphql_countries_i"] = context["graphql_countries_i"] + 1
      local countries = context["graphql_countries"][context["graphql_countries_i"]]
      local json = cjson.decode(html)
      local found = extract_all_ads(json)
      local ad_library = json["data"]["ad_library_main"]
      if ad_library then
        if found == 0 then
          wget.callbacks.finish()
          print("You are likely banned temporarily. Sleeping 600 seconds.")
          io.stdout:flush()
          os.execute("sleep 600")
          error()
        end
        local search_results = ad_library["search_results_connection"]
        if search_results["page_info"]["has_next_page"] then
          local variables = copy_table(context["graphql_variables"])
          variables["cursor"] = search_results["page_info"]["end_cursor"]
          variables["countries"] = countries
          make_graphql_request(
            "AdLibrarySearchPaginationQuery",
            variables
          )
        end
        return urls
      end
      html = flatten_json(json)
    end
    for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if http_stat["statcode"] ~= 200 then
    retry_url = true
    return false
  end
  if http_stat["len"] == 0
    and http_stat["statcode"] < 300 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 8
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      if not seen_200[url["url"]] then
        seen_200[url["url"]] = 0
      end
      seen_200[url["url"]] = seen_200[url["url"]] + 1
    end
    downloaded[url["url"]] = true
  end

  if string.match(url["url"], "graphql") then
    os.execute("sleep 1")
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["metaadlibrary-nhwx7mmrme8nj52e"] = discovered_items,
    ["urls-omhd9lqfegay6nvp"] = discovered_outlinks
  }) do
    print("queuing for", string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


