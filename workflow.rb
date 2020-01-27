require 'rbbt'
require 'rbbt/workflow'

module GnomAD
  extend Workflow

  def self.organism(reference)
    Organism.organism_for_build reference
  end

  input :mutations, :array, "Genomic Mutation", nil, :stream => true
  input :by_position, :boolean, "Identify by position", false
  input :reference, :select, "Human reference", 'hg38', :select_options => %w(hg19 bc37 hg38)
  task :annotate => :tsv do |mutations,by_position,reference|
    database = GnomAD.database(reference)
    organism = GnomAD.organism(reference)
    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => database.fields, :type => (by_position ? :double : :list), :organism => organism
    dumper.init
    database.unnamed = true
    TSV.traverse mutations, :type => :array, :into => dumper, :bar => self.progress_bar("Annotate GnomAD") do |mutation|
      if by_position
        position = mutation.sub(/^chr/, '').split(":").values_at(0,1) * ":"
        keys = database.prefix(position + ":")
        next if keys.nil?
        values = keys.collect{|key| database[key].collect{|v| v.nil? ? nil : v.gsub("|", ";") } }.uniq
        [mutation, Misc.zip_fields(values)]
      else
        values = database[mutation.sub(/^chr/, '')]
        next if values.nil?
        values = values.collect{|v| v.nil? ? nil : v.gsub("|", ";") } 
        [mutation, values]
      end
    end
  end
end

require 'rbbt/sources/GnomAD'
