module CouchRestRails
  module Tests

    extend self
    mattr_accessor :fixtures_loaded
    self.fixtures_loaded = Set.new

    def setup(database="*")
      ENV['RAILS_ENV'] = CouchRestRails.test_environment
      database.each { |db|
        unless fixtures_loaded.include?(db)
          CouchRestRails::Database.create(db)
          CouchRestRails::Fixtures.clear(db)
          CouchRestRails::Fixtures.load(db)
          fixtures_loaded << db
        end
      }
    end

    def reset_fixtures
      CouchRestRails::Database.delete("*") unless fixtures_loaded.empty?
      fixtures_loaded.clear
    end

    def teardown(database="*")
      ENV['RAILS_ENV'] = CouchRestRails.test_environment
      database.each { |db|
        CouchRestRails::Fixtures.clear(db)
        fixtures_loaded.delete(db)
      }
    end
  end

  module ReadOnlyTestBehavior
    def self.included(base)
      base.send :cattr_accessor, :expected_test_count

      base.send :superclass_delegating_accessor, :database
      base.database = nil

      base.send :setup, :count_tests_and_setup_couchdb
      base.send :teardown, :teardown_couchdb_if_finished

      class << base
        def couchdb_fixtures(*databases)
          self.database = databases.map { |d| d.to_s }
        end
        def setup_couchdb
          CouchRestRails::Tests.setup(self.database) unless self.database.nil?
        end
        def teardown_couchdb
          CouchRestRails::Tests.teardown(self.database) unless self.database.nil?
        end

        # Override these methods in your test class to provide custom one-time
        # setup and teardown logic.
        def global_setup; end
        def global_teardown; end
      end

      def teardown_couchdb_if_finished
        if (self.class.expected_test_count -= 1) == 0
          self.class.global_teardown
          self.class.teardown_couchdb
          self.class.expected_test_count = nil
        end
      end

      def count_tests_and_setup_couchdb
        cls = self.class
        unless cls.expected_test_count
          cls.expected_test_count = (cls.instance_methods.reject{|method| method[0..3] != 'test'}).length
          cls.setup_couchdb
          cls.global_setup
        end
      end

      # Override the following to short-circuit per test setup/teardown code
      def setup_couchdb_fixtures; end
      def teardown_couchdb_fixtures; end
    end
  end

  #
  # Tests against CouchDB are SLOW due to the setup and teardown process.  If
  # your tests do not alter the database, consider extending one of these
  # classes for faster tests.
  #
  class ReadOnlyActiveSupportTest < ActiveSupport::TestCase
    include ReadOnlyTestBehavior
  end

  class ReadOnlyActionControllerTest < ActionController::TestCase
    include ReadOnlyTestBehavior
  end
end

class ActiveSupport::TestCase
  setup :setup_couchdb_fixtures
  teardown :teardown_couchdb_fixtures

  superclass_delegating_accessor :database
  self.database = nil

  class << self
    def couchdb_fixtures(*databases)
      self.database = databases.map { |d| d.to_s }
    end
  end
  def setup_couchdb_fixtures
    CouchRestRails::Tests.setup(self.database) unless self.database.nil?
  end
  def teardown_couchdb_fixtures
    CouchRestRails::Tests.teardown(self.database) unless self.database.nil?
  end
end
