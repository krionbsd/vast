#include <vast/ingest/reader.h>

#include <ze/event.h>
#include <vast/ingest/exception.h>
#include <vast/util/logger.h>

namespace vast {
namespace ingest {

reader::reader(cppa::actor_ptr upstream, std::string const& filename)
  : upstream_(upstream)
  , file_(filename)
{
  LOG(verbose, ingest)
    << "spawning reader @" << id()
    << " for file " << filename
    << " with upstream @" << upstream_->id();

  if (file_)
    file_.unsetf(std::ios::skipws);
  else
    LOG(error, ingest) << "reader @" << id() << " cannot read " << filename;

  using namespace cppa;
  chaining(false);
  init_state = (
      on(atom("extract"), arg_match) >> [=](size_t batch_size)
      {
        if (! file_)
        {
          LOG(error, ingest)
            << "reader @" << id() << " experienced an error with " << filename;

          reply(atom("reader"), atom("done"));
        }

        auto events = std::move(extract(batch_size));
        if (! events.empty())
        {
          total_events_ += events.size();

          LOG(verbose, ingest)
            << "reader @" << id()
            << " sends " << events.size()
            << " events to @" << upstream_->id()
            << " (cumulative events: " << total_events_ << ')';

          send(upstream_, std::move(events));
        }

        reply(atom("reader"), file_ ? atom("ack") : atom("done"));
      },
      on(atom("shutdown")) >> [=]
      {
        self->quit();
        LOG(verbose, ingest) << "reader @" << id() << " terminated";
      });
}

line_reader::line_reader(cppa::actor_ptr upstream, std::string const& filename)
  : reader(std::move(upstream), filename)
{
}

std::vector<ze::event> line_reader::extract(size_t batch_size)
{
  std::vector<ze::event> events;
  events.reserve(batch_size);

  size_t errors = 0;
  std::string line;
  while (std::getline(file_, line))
  {
    try
    {
      ++current_line_;
      if (line.empty())
        continue;

      events.emplace_back(parse(line));

      if (events.size() == batch_size)
        break;
    }
    catch (parse_exception const& e)
    {
      LOG(warn, ingest)
        << "reader @" << id() << " encountered parse error at line " << current_line_
        << ": " << e.what();

      if (++errors >= 20)
        break;
    }
  }

  return events;
}

bro_reader::bro_reader(cppa::actor_ptr upstream, std::string const& filename)
  : line_reader(std::move(upstream), filename)
{
  parse_header();
}

void bro_reader::parse_header()
{
  std::string line;


  {
    if (file_.peek() != '#')
      throw parse_exception("bro log lacks first log file header");
    if (! std::getline(file_, line))
      throw parse_exception("could not extract first log line");

    field_splitter<std::string::const_iterator> fs;
    fs.split(line.begin(), line.end());
    if (fs.fields() != 2 || std::string(fs.start(0), fs.end(0)) != "#separator")
      throw parse_exception("invalid #separator definition");

    std::string sep;
    std::string bro_sep(fs.start(1), fs.end(1));
    std::string::size_type pos = 0;
    while (pos != std::string::npos)
    {
      pos = bro_sep.find("\\x", pos);
      if (pos != std::string::npos)
      {
        auto i = std::stoi(bro_sep.substr(pos + 2, 2), nullptr, 16);
        assert(i >= 0 && i <= 255);
        sep.push_back(i);
        pos += 2;
      }
    }

    separator_.assign(sep.begin(), sep.end());
  }

  {
    if (file_.peek() != '#')
      throw parse_exception("bro log lacks second log file header");
    if (! std::getline(file_, line))
      throw parse_exception("could not extract second log line");

    field_splitter<std::string::const_iterator> fs;
    fs.sep(separator_.data(), separator_.size());
    fs.split(line.begin(), line.end());
    if (fs.fields() != 2 || std::string(fs.start(0), fs.end(0)) != "#set_separator")
      throw parse_exception("invalid #set_separator definition");

    // FIXME: support multi-char set separators.
    auto set_sep = std::string(fs.start(1), fs.end(1));
    set_separator_.assign(set_sep.begin(), set_sep.end());
  }

  {
    if (file_.peek() != '#')
      throw parse_exception("bro log lacks third log file header");
    if (! std::getline(file_, line))
      throw parse_exception("could not extract third log line");

    field_splitter<std::string::const_iterator> fs;
    fs.sep(separator_.data(), separator_.size());
    fs.split(line.begin(), line.end());
    if (fs.fields() != 2 || std::string(fs.start(0), fs.end(0)) != "#empty_field")
      throw parse_exception("invalid #empty_field definition");

    auto empty = std::string(fs.start(1), fs.end(1));
    empty_field_.assign(empty.begin(), empty.end());
  }

  {
    if (file_.peek() != '#')
      throw parse_exception("bro log lacks fourth log file header");
    if (! std::getline(file_, line))
      throw parse_exception("could not extract fourth log line");

    field_splitter<std::string::const_iterator> fs;
    fs.sep(separator_.data(), separator_.size());
    fs.split(line.begin(), line.end());
    if (fs.fields() != 2 || std::string(fs.start(0), fs.end(0)) != "#unset_field")
      throw parse_exception("invalid #unset_field definition");

    auto unset = std::string(fs.start(1), fs.end(1));
    unset_field_.assign(unset.begin(), unset.end());
  }

  {
    if (file_.peek() != '#')
      throw parse_exception("bro log lacks fifth log file header");
    if (! std::getline(file_, line))
      throw parse_exception("could not extract fifth log line");

    field_splitter<std::string::const_iterator> fs;
    fs.sep(separator_.data(), separator_.size());
    fs.split(line.begin(), line.end());
    if (fs.fields() != 2 || std::string(fs.start(0), fs.end(0)) != "#path")
      throw parse_exception("invalid #path definition");

    auto path = std::string(fs.start(1), fs.end(1));
    path_.assign(path.begin(), path.end());
  }

  {
    if (file_.peek() != '#')
      throw parse_exception("bro log lacks sixth log file header");
    if (! std::getline(file_, line))
      throw parse_exception("could not extract sixth log line");

    field_splitter<std::string::const_iterator> fs;
    fs.sep(separator_.data(), separator_.size());
    fs.split(line.begin(), line.end());

    for (size_t i = 1; i < fs.fields(); ++i)
      field_names_.emplace_back(fs.start(i), fs.end(i));
  }

  {
    if (file_.peek() != '#')
      throw parse_exception("bro log lacks seventh log file header");
    if (! std::getline(file_, line))
      throw parse_exception("could not seventh sixth log line");

    field_splitter<std::string::const_iterator> fs;
    fs.sep(separator_.data(), separator_.size());
    fs.split(line.begin(), line.end());

    for (size_t i = 1; i < fs.fields(); ++i)
    {
      std::string type(fs.start(i), fs.end(i));
      ze::value_type vt;
      // TODO: complete to all types
      if (type == "enum")
        vt = ze::string_type;
      else if (type == "bool")
        vt = ze::bool_type;
      else if (type == "count")
        vt = ze::uint_type;
      else if (type == "interval")
        vt = ze::duration_type;
      else if (type == "time")
        vt = ze::timepoint_type;
      else if (type == "string")
        vt = ze::string_type;
      else if (type == "addr")
        vt = ze::address_type;
      else if (type == "port")
        vt = ze::port_type;

      field_types_.push_back(vt);
    }
  }

  if (file_.peek() == '#')
    throw parse_exception("more headers than VAST knows");

  DBG(ingest) 
    << "reader @" << id() << " parsed bro2 header:"
    << " #separator " << separator_
    << " #set_separator " << set_separator_
    << " #empty_field " << empty_field_
    << " #unset_field " << unset_field_
    << " #path " << path_;
  {
    std::ostringstream str;
    for (auto& name : field_names_)
      str << " " << name;
    DBG(ingest) << "reader @" << id() << " extracted field names:" << str.str();
  }
  {
    std::ostringstream str;
    for (auto& type : field_types_)
      str << " " << type;
    DBG(ingest) << "reader @" << id() << " extracted field types:" << str.str();
  }
}

ze::event bro_reader::parse(std::string const& line)
{
  assert(! "not yet implemented");
}

bro_15_conn_reader::bro_15_conn_reader(cppa::actor_ptr upstream,
                                       std::string const& filename)
  : line_reader(std::move(upstream), filename)
{
}

ze::event bro_15_conn_reader::parse(std::string const& line)
{
  // A connection record.
  ze::event e("bro::connection");
  e.timestamp(ze::clock::now());
  // FIXME: Improve performance of random UUID generation.
  //  e.id(ze::uuid::random());

  field_splitter<std::string::const_iterator> fs;
  fs.split(line.begin(), line.end(), 13);
  if (! (fs.fields() == 12 || fs.fields() == 13))
    throw parse_exception("not enough conn.log fields (at least 12 needed)");

  // Timestamp
  auto i = fs.start(0);
  auto j = fs.end(0);
  e.emplace_back(ze::value::parse_time_point(i, j));
  if (i != j)
    throw parse_exception("invalid conn.log timestamp (field 1)");

  // Duration
  i = fs.start(1);
  j = fs.end(1);
  if (*i == '?')
  {
    e.emplace_back(ze::nil);
  }
  else
  {
    e.emplace_back(ze::value::parse_duration(i, j));
    if (i != j)
      throw parse_exception("invalid conn.log duration (field 2)");
  }

  // Originator address
  i = fs.start(2);
  j = fs.end(2);
  e.emplace_back(ze::value::parse_address(i, j));
  if (i != j)
    throw parse_exception("invalid conn.log originating address (field 3)");

  // Responder address
  i = fs.start(3);
  j = fs.end(3);
  e.emplace_back(ze::value::parse_address(i, j));
  if (i != j)
    throw parse_exception("invalid conn.log responding address (field 4)");

  // Service
  i = fs.start(4);
  j = fs.end(4);
  if (*i == '?')
  {
    e.emplace_back(ze::nil);
  }
  else
  {
    e.emplace_back(ze::value(i, j));
    if (i != j)
      throw parse_exception("invalid conn.log service (field 5)");
  }

  // Ports and protocol
  i = fs.start(5);
  j = fs.end(5);
  auto orig_p = ze::value::parse_port(i, j);
  if (i != j)
    throw parse_exception("invalid conn.log originating port (field 6)");

  i = fs.start(6);
  j = fs.end(6);
  auto resp_p = ze::value::parse_port(i, j);
  if (i != j)
    throw parse_exception("invalid conn.log responding port (field 7)");

  i = fs.start(7);
  j = fs.end(7);
  auto proto = ze::value(i, j);
  if (i != j)
    throw parse_exception("invalid conn.log proto (field 8)");

  auto str = proto.get<ze::string>().data();
  auto len = proto.get<ze::string>().size();
  auto p = ze::port::unknown;
  if (! std::strncmp(str, "tcp", len))
    p = ze::port::tcp;
  else if (! std::strncmp(str, "udp", len))
    p = ze::port::udp;
  else if (! std::strncmp(str, "icmp", len))
    p = ze::port::icmp;
  orig_p.get<ze::port>().type(p);
  resp_p.get<ze::port>().type(p);
  e.emplace_back(std::move(orig_p));
  e.emplace_back(std::move(resp_p));
  e.emplace_back(std::move(proto));

  // Originator and responder bytes
  i = fs.start(8);
  j = fs.end(8);
  if (*i == '?')
  {
    e.emplace_back(ze::value::parse_uint(i, j));
  }
  else
  {
    e.emplace_back(ze::value::parse_duration(i, j));
    if (i != j)
      throw parse_exception("invalid conn.log originating bytes (field 9)");
  }

  i = fs.start(8);
  j = fs.end(8);
  if (*i == '?')
  {
    e.emplace_back(ze::value::parse_uint(i, j));
  }
  else
  {
    e.emplace_back(ze::value::parse_duration(i, j));
    if (i != j)
      throw parse_exception("invalid conn.log responding bytes (field 10)");
  }

  // Connection state
  i = fs.start(10);
  j = fs.end(10);
  e.emplace_back(i, j);
  if (i != j)
    throw parse_exception("invalid conn.log connection state (field 11)");

  // Direction
  i = fs.start(11);
  j = fs.end(11);
  e.emplace_back(i, j);
  if (i != j)
    throw parse_exception("invalid conn.log direction (field 12)");

  // Additional information
  if (fs.fields() == 13)
  {
    i = fs.start(12);
    j = fs.end(12);
    e.emplace_back(i, j);
    if (i != j)
      throw parse_exception("invalid conn.log direction (field 12)");
  }

  return e;
}

} // namespace ingest
} // namespace vast
