require 'rbbt-util'
require 'rbbt/util/open'
require 'rbbt/resource'
require 'rbbt/sources/organism'

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '../../..', 'lib'))

module GnomAD
  extend Resource
  self.subdir = "var/GnomAD"

  def self.vcf_database(vcf_file, options = {})
    options = Misc.add_defaults options, :persist => true, :type => :list, :engine => 'BDB'
    db = Persist.persist_tsv("VCF Database", vcf_file, {}, options) do |data|
      Workflow.require_workflow "Sequence"
      parser = TSV::Parser.new Sequence::VCF.open_stream(Open.open(vcf_file, :nocache => true), false, false, true)

      data.fields = parser.fields
      data.key_field = parser.key_field
      data.serializer = :list

      TSV.traverse parser, :bar => "Creating VCF Database" do |mutation,values|
        chr, position, alt = mutation.split(":")
        mutations = alt.split(",").collect{|mut|
          [chr, position, mut] * ":"
        }

        mutations.each do |mutation|
          data[mutation] = values
        end
      end

      data
    end
  end
  %w(hg19 hg38).each do |reference|
    GnomAD.claim GnomAD[reference], :proc do |filename|
      vcf_database(vcf, :file => filename)
      nil
    end
  end


  def self.database(reference)
    vcf = case reference
          when 'b37', 'hg19'
            Organism.Hsa.b37.known_sites["af-only-gnomad.vcf.gz"].find
          when 'hg38'
            Organism.Hsa.hg38.known_sites["af-only-gnomad.vcf.gz"].find
          else
            raise "VCF not found for reference: #{reference}"
          end

    @@database ||= begin
                     vcf_database(vcf)
                   end
  end
end
