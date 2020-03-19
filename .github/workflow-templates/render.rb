#!/usr/bin/env ruby

# https://github.community/t5/GitHub-Actions/Support-for-YAML-anchors/m-p/42517/highlight/true#M5024

require "yaml"
require "json"
require "erb"

header = """# THIS FILE IS AUTOMATICALLY GENERATED
# DO NOT EDIT
"""

ginkgo_skip='--ginkgo.skip=Networking\sIPerf\sIPv[46]|\[Feature:PerformanceDNS\]|\[Feature:IPv6DualStackAlphaFeature\]|NetworkPolicy\sbetween\sserver\sand\sclient.+(ingress\saccess|multiple\segress\spolicies|allow\segress\saccess)|\[Feature:NoSNAT\]|Services.+(ESIPP|cleanup\sfinalizer|session\saffinity)|\[Feature:Networking-IPv6\]|\[Feature:Federation\]|configMap\snameserver|ClusterDns\s\[Feature:Example\]|(Namespace|Pod)Selector\s\[Feature:NetworkPolicy\]|kube-proxy|should\sset\sTCP\sCLOSE_WAIT\stimeout'

rendered_out = ERB.new(File.read(File.expand_path("test.yml.erb", __dir__))).result()
yaml_out = YAML.load(rendered_out)
puts "rendered yaml is: "
puts YAML.dump(yaml_out)
File.write(File.expand_path("../workflows/test_generated.yml", __dir__), header + YAML.load(yaml_out.to_json).to_yaml())
