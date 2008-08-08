package DBIx::ContentChecksum;

use 5.006;
use strict;
use warnings;

our $VERSION = '1.0';

{ package db_comparison;

	sub new {
		my $class = shift;
		my $self = { _primary_keys => {} };
		bless $self, $class;
		if (@_){
			$self->set_dbh(@_);
			if (my $new_class = $self->guess_class){
				bless $self,$new_class;
			}
		} else {
			die "DBIx::ContentChecksum ERROR: Need two database handles to compare\n";		
		}
		return $self;
	}
	sub guess_class {
		my $self = shift;
		my ($dbh1,$dbh2) = $self->get_dbh;
		my $driver_class1 = $dbh1->{Driver}->{Name};
		my $driver_class2 = $dbh2->{Driver}->{Name};
		die "ERROR: DB drivers must be the same type in the current implementation of DBIx::ContentChecksum\n" 
			unless ($driver_class1 eq $driver_class2);
			
		eval "use DBIx::ContentChecksum::$driver_class1";
		if ($@){
			warn "Could not load module DBIx::ContentChecksum::$driver_class1\nSimple comparison only available\n";
		} else {
			return "db_comparison_".$driver_class1;
		}
	}
	sub set_dbh {
		my $self = shift;
		my ($dbh1,$dbh2) = @_;
		die "DBIx::ContentChecksum ERROR: Need two database handles to compare\n" unless ($dbh2);
		die "DBIx::ContentChecksum ERROR: Not a database handle\n"
			unless (($dbh1->isa('DBI::db')) && ($dbh2->isa('DBI::db')));
		$self->{ _dbh } = [$dbh1,$dbh2];
	}
	sub get_dbh {
		my $self = shift;
		return @{ $self->{ _dbh } }; 
	}
	sub get_db_names {
		my $self = shift;
		my ($dbh1,$dbh2) = $self->get_dbh;
		return ($dbh1->{ Name },$dbh2->{ Name });
	}
	sub get_tables {
		my $self = shift;
		unless (defined $self->{ _tables }){
			my ($dbh1,$dbh2) = $self->get_dbh;
			my $aTables1 = $self->fetch_multisinglefield("show tables",$dbh1);
			my $aTables2 = $self->fetch_multisinglefield("show tables",$dbh2);
			$self->{ _tables } = [$aTables1,$aTables2];
		}
		if (wantarray()){
			return ( $self->{ _tables }[0],$self->{ _tables }[1] );
		} else {
			return $self->{ _tables }[0];
		}
	}
	sub get_differences {
		my $self = shift;
		if (@_){
			my $type = shift;
			unless (defined $self->{ _error_list }{ $type }){
				$self->{ _error_list }{ $type } = [];
			}
			return $self->{ _error_list }{ $type };
		} else {
			unless (defined $self->{ _error_list }){
				$self->{ _error_list } = {};
			}
			return $self->{ _error_list };
		}
	}
	sub add_errors {
		my $self = shift;
		my $aErrors = $self->get_differences(shift);
		push (@$aErrors, @_);
	}
	sub compare {
		my $self = shift;
		
		my $tables = $self->compare_table_lists;
		my $rows = $self->compare_row_counts;

		my $hDiffs = $self->get_differences;
		if (defined wantarray()) {
		    return $hDiffs;
		} else {
			unless ($rows){
				print 	"Row counts in some tables are different\n".
						"\tComparing the content of tables with the same row count...\n";
			}	
			unless ($tables){
				print 	"Table Lists are different\n".
						"\tComparing the common tables...\n";
			}
			unless ($tables && $rows){
				if (%$hDiffs){
					while (my ($type,$aErrors) = each %$hDiffs){
						print "$type:\n";
						for my $error (@$aErrors){
							print "\t$error\n";
						}
					}
				}
			}
		}
	}
	sub compare_table_lists {
		my $self = shift;
		my ($aTables1,$aTables2) = $self->get_tables;
		if (join(',',@$aTables1) eq join(',',@$aTables2)){
			return 1;
		} else {
			$self->find_table_diffs;
			return;
		}
	}
	sub find_table_diffs {
		my $self = shift;
		my ($aTables1,$aTables2) = $self->get_tables;
		my ($dbh1,$dbh2) = $self->get_dbh;
		
		my (%hTables1,%hTables2,@aNotIn1,@aNotIn2,@aInBoth);
		
		for my $table1 (@$aTables1){
			$hTables1{ $table1 }++;
		}
		for my $table2 (@$aTables2){
			$hTables2{ $table2 }++;
			if (defined $hTables1{ $table2 }){
				push(@aInBoth,$table2);
			} else {
				push(@aNotIn1,$table2);
			}
		}
		for my $table1 (@$aTables1){
			unless (defined $hTables2{ $table1 }){
				push(@aNotIn2,$table1);
			}
		}
		$self->{ _tables } = [\@aInBoth];
		my ($db1,$db2) = $self->get_db_names;
		$self->add_errors("Tables unique to $db1",@aNotIn2) if (@aNotIn2);
		$self->add_errors("Tables unique to $db2",@aNotIn1) if (@aNotIn1);
	}
	
	sub compare_row_counts {
		my $self = shift;
		my ($dbh1,$dbh2) = $self->get_dbh;
		my ($aTables,@aErrors,@aOK_Tables);
		if (@_){
			$aTables = [shift];
		} else {
			$aTables = $self->get_tables;
		}
		TABLE:for my $table (@$aTables){
			if ($self->row_count($table,$dbh1) != $self->row_count($table,$dbh2)){
				push(@aErrors,$table);
			} else {
				push(@aOK_Tables,$table);
			}
		}
		if (@aErrors){
			$self->{ _tables } = [\@aOK_Tables];	# reset ready for checksum
			$self->add_errors('Row count',@aErrors);
			return;
		} else {
			return 1;
		}
	}
	sub get_primary_keys {
		my $self = shift;
		my $table = shift;
		my $dbh = shift;
		my $db = $dbh->{ Name }; 	# actually name:host
		unless (defined $self->{ _primary_keys }{ "$db.$table" }){
			$self->set_primary_keys($table,$dbh);
		} 
		my $aKeys = $self->{ _primary_keys }{ "$db.$table" };
		if (@$aKeys){
			if (wantarray()){
				return @$aKeys;
			} else {
				return join(',',@$aKeys);
			}
		} else {
			return;
		}
	}
	sub set_primary_keys {
		my $self = shift;
		my $table = shift;
		my $dbh = shift;
		my $db = $dbh->{ Name }; 	# actually name:host
		my ($db_name,$host) = split(/:/,$db);	
		my $sth = $dbh->primary_key_info( $db_name,undef,$table );
		my $hhResults = $sth->fetchall_hashref('KEY_SEQ');
		$sth->finish;

		my @aKeys = ();
		while (my ($key_seq,$hKey) = each %$hhResults) {
			$aKeys[$key_seq-1] = $hKey->{COLUMN_NAME};
		}
		$self->{ _primary_keys }{ "$db.$table" } = \@aKeys;
	}
	sub row_count {
		my $self = shift;
		my $table = shift;
		return $self->fetch_singlefield("select count(*) from $table",shift);	# shifting $dbh
	}
	sub fields_hash {
		my $self = shift;
		my $table = shift;
		my $statement = "DESCRIBE $table";
		return $self->fetchhash_multirow($statement,shift);
	}
	sub fetchhash_multirow {
		my $self = shift;
		my $statement = shift;
		my $dbh = shift;		
		my @ahResults_Rows;
		eval {
			my $sth = $dbh->prepare($statement);
			$sth->execute(); 
			while(my $hResult_Row = $sth->fetchrow_hashref()) {
				push @ahResults_Rows, $hResult_Row;
			}
			$sth->finish(); # we're done with this query
		};
		if ($@) {
			die $@;
		} else {
			return \@ahResults_Rows;
		}
	}	
	sub fetch_multisinglefield {
		my $self = shift;
		my $statement = shift;
		my $dbh = shift;		
		my @aValues;
		my $value;
		eval {
			my $sth = $dbh->prepare($statement);
			$sth->execute(); 
			$sth->bind_columns(undef, \$value);
			while($sth->fetch()) {
				push @aValues, $value;
			}
			$sth->finish(); 
		};
		if ($@) {
			die $@;
		} else {
			return \@aValues;
		} 
	} 
	sub fetch_singlefield {
		my $self = shift;
		my $statement = shift;
		my $dbh = shift;	
		my $value;
		eval {
			my $sth = $dbh->prepare($statement);
			$sth->execute(); 
			$sth->bind_columns(undef, \$value);
			$sth->fetch();
			$sth->finish(); 
		};
		if ($@) {
			die $@;
		} else {
			return $value;
		}
	}

}

1;

__END__


=head1 NAME

DBIx::ContentChecksum - Compare database content

=head1 SYNOPSIS

	use DBIx::ContentChecksum;

	my $oDB_Comparison = db_comparison->new($dbh1,$dbh2);
	$oDB_Comparison->compare;

=head1 DESCRIPTION

DBIx::ContentChecksum takes two database handles and performs a very low level comparison of their table content. 

=head1 METHODS

=over

=item B<new($dbh1,$dbh2)>

You must pass two database handles at initialisation, and each database must be the same type. 

=item B<compare>

Performs the comparison. Calls the methods compare_table_lists and compare_row_counts. In scalar context, returns a hashref of the differences found. In void context this method outputs a report to STDOUT. 

=item B<compare_table_lists>

Simple comparison of the table names. Returns true if no differences are found, otherwise returns undef. An array ref of tables unique to each database:host can be recovered with get_differences(), using the hash key C<'Tables unique to I<[db name:host]>'>

=item B<compare_row_counts>

Comparison of the row counts from each table. Can pass a table name, or will compare all tables. Returns true if no differences are found, otherwise returns undef. An array ref of tables with different row counts can be recovered with get_differences(), using the hash key C<'Row count'>. 

=item B<get_primary_keys($table,$dbh)>

Returns the primary keys (in key order) for the given table/database, either as a list or as a comma separated string. 

=item B<get_differences>

Returns a hashref of differences between the two databases, where keys are the source of the difference, and values are an array ref of the differences found (see comparison methods above for details).

=item B<get_tables>

Returns a table list. Before a comparison has been run, this method will return a 2D list of tables in list context, or just a list of tables in database1 in scalar context;

	my @aList = $oDB_Comparison->get_tables;	# returns (['table1','table2',etc],['table1','table2',etc])

However, after a comparison has been run, only those tables that are the same for the comparison are returned by a subsequent call to get_tables(). 

=back

=head1 FUTURE DEVELOPMENT

At some point (probably when I need to) I intend to expand this module to perform row-by-row comparison. 

=head1 AUTHOR

Christopher Jones, Gynaecological Cancer Research Laboratories, UCL EGA Institute for Women's Health, University College London.

c.jones@ucl.ac.uk

=head1 COPYRIGHT AND LICENSE

Copyright 2008 by Christopher Jones, University College London

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself. 

=cut
