require 'active_record'
require 'active_record/base'
require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/relation.rb'
require 'active_record/persistence.rb'
require 'active_record/relation/query_methods.rb'

#
# Patching {ActiveRecord} to allow specifying the table name as a function of
# attributes.
#
module ActiveRecord
  #
  # Patches for Persistence to allow certain partitioning (that related to the primary key) to work.
  # Monkeypatch based on:
  # https://github.com/rails/rails/blob/4-2-stable/activerecord/lib/active_record/persistence.rb
  #
  module Persistence
    # This method is patched to provide a relation referencing the partition instead
    # of the parent table.
    def _delete_record
	  constraints = _substitute_values(constraints).map { |attr, bind| attr.eq(bind) }
	  dm = Arel::DeleteManager.new

      # ****** BEGIN PARTITIONED PATCH ******
      if self.class.respond_to?(:dynamic_arel_table)
        dm.from(dynamic_arel_table)
      else
      # ****** END PARTITIONED PATCH ******

        dm.from(arel_table)

      # ****** BEGIN PARTITIONED PATCH ******
      end
      # ****** END PARTITIONED PATCH ******

	  dm.wheres = constraints

	  connection.delete(dm, "#{self} Destroy")
    end

    # This method is patched to use a table name that is derived from
    # the attribute values.
    def _insert_record(values)
      primary_key_value = nil

      if primary_key && Hash === values
		primary_key_value = values[primary_key]

		if !primary_key_value && prefetch_primary_key?
		  primary_key_value = next_sequence_value
		  values[primary_key] = primary_key_value
		end
      end

      # ****** BEGIN PARTITIONED PATCH ******
      actual_arel_table = @klass.dynamic_arel_table(Hash[*values.map{|k,v| [k.name,v]}.flatten]) if @klass.respond_to?(:dynamic_arel_table)
      actual_arel_table = @table unless actual_arel_table

      if values.empty?
        im = actual_arel_table.compile_insert(connection.empty_insert_statement_value)
        im.into actual_arel_table
      else
        im = actual_arel_table.compile_insert(_substitute_values(values))
      end
      # ****** END PARTITIONED PATCH ******

      connection.insert(im, "#{self} Create", primary_key || false, primary_key_value)
    end

    def _update_record(values, constraints) # :nodoc:
      constraints = _substitute_values(constraints).map { |attr, bind| attr.eq(bind) }

      # ****** BEGIN PARTITIONED PATCH ******
      if @klass.respond_to?(:dynamic_arel_table)
        using_arel_table = @klass.dynamic_arel_table(Hash[*values.map { |k,v| [k.name,v] }.flatten])
        um = using_arel_table.where(
          constraints.reduce(&:and)
        ).compile_update(_substitute_values(values), primary_key)
      else
        # ****** END PARTITIONED PATCH ******
        um = arel_table.where(
          constraints.reduce(&:and)
        ).compile_update(_substitute_values(values), primary_key)
        # ****** BEGIN PARTITIONED PATCH ******
      end

      # NOTE(hofer): The um variable got set up using
      # klass.arel_table as its arel value.  So arel_table.name is
      # what gets used to construct the update statement.  Here we
      # set it to the specific partition name for this record so
      # that the update gets run just on that partition, not on the
      # parent one (which can cause performance issues).
      begin
        @klass.arel_table.name = using_arel_table.name
        # ****** END PARTITIONED PATCH ******
        connection.update(um, "#{self} Update")
        # ****** BEGIN PARTITIONED PATCH ******
      ensure
        @klass.arel_table.name = @klass.table_name
      end
      # ****** END PARTITIONED PATCH ******
    end

    private

    # This method is patched to prefetch the primary key (if necessary) and to ensure
    # that the partitioning attributes are always included (AR will exclude them
    # if the db column's default value is the same as the new record's value).
    def _create_record(attribute_names = self.attribute_names)
      # ****** BEGIN PARTITIONED PATCH ******
      if self.id.nil? && self.class.respond_to?(:prefetch_primary_key?) && self.class.prefetch_primary_key?
        self.id = self.class.connection.next_sequence_value(self.class.sequence_name)
        attribute_names |= ["id"]
      end

      if self.class.respond_to?(:partition_keys)
        attribute_names |= self.class.partition_keys.map(&:to_s)
      end
      # ****** END PARTITIONED PATCH ******

      attributes_values = attributes_with_values_for_create(attribute_names)

      new_id = self.class._insert_record(attributes_values)
      self.id ||= new_id if self.class.primary_key

      @new_record = false

      yield(self) if block_given?

      id
    end
    # Updates the associated record with values matching those of the instance attributes.
    # Returns the number of affected rows.
    def _update_record(attribute_names = self.attribute_names)
      # ****** BEGIN PARTITIONED PATCH ******
      # NOTE(hofer): This patch ensures the columns the table is
      # partitioned on are passed along to the update code so that the
      # update statement runs against a child partition, not the
      # parent table, to help with performance.
      if self.class.respond_to?(:partition_keys)
        attribute_names.concat self.class.partition_keys.map(&:to_s)
        attribute_names.uniq!
      end
      # ****** END PARTITIONED PATCH ******
      attributes_names = attributes_for_update(attribute_names)
      if attributes_names.empty?
		affected_rows = 0
        @_trigger_update_callback = true
      else
        affected_rows = _update_row(attribute_names)
		@_trigger_update_callback = affected_rows == 1
      end

      yield(self) if block_given?

      affected_rows
    end


  end # module Persistence

  module QueryMethods

    # This method is patched to change the default behavior of select
    # to use the Relation's Arel::Table
    # Monkeypatch based on: https://github.com/rails/rails/blob/4-2-stable/activerecord/lib/active_record/relation/query_methods.rb
    def build_select(arel)
      if select_values.any?
        # ****** BEGIN PARTITIONED PATCH ******
        # Original line:
        # arel.project(*arel_columns(select_values.uniq))
        expanded_select = select_values.map do |field|
          columns_hash.key?(field.to_s) ? arel_table[field] : field
        end
        arel.project(*expanded_select)
        # ****** END PARTITIONED PATCH ******
      else
        # ****** BEGIN PARTITIONED PATCH ******
        # Original line:
        # arel.project(@klass.arel_table[Arel.star])
        arel.project(table[Arel.star])
        # ****** END PARTITIONED PATCH ******
      end
    end

  end # module QueryMethods
end # module ActiveRecord
