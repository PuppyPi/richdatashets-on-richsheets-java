package rebound.richdatashets.impl.richsheets;

import static java.util.Collections.*;
import static rebound.testing.WidespreadTestingUtilities.*;
import static rebound.text.StringUtilities.*;
import static rebound.util.collections.CollectionUtilities.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import rebound.richdatashets.api.model.RichdatashetsCellAbsenceStrategy;
import rebound.richdatashets.api.model.RichdatashetsRow;
import rebound.richdatashets.api.model.RichdatashetsSemanticColumns;
import rebound.richdatashets.api.model.RichdatashetsTable;
import rebound.richdatashets.api.operation.RichdatashetsConnection;
import rebound.richdatashets.api.operation.RichdatashetsMaintenanceFixableStructureException;
import rebound.richdatashets.api.operation.RichdatashetsOperation;
import rebound.richdatashets.api.operation.RichdatashetsOperation.RichdatashetsOperationWithDataTimestamp;
import rebound.richdatashets.api.operation.RichdatashetsStructureException;
import rebound.richdatashets.api.operation.RichdatashetsUnencodableFormatException;
import rebound.richsheets.api.model.RichsheetsRow;
import rebound.richsheets.api.model.RichsheetsTable;
import rebound.richsheets.api.operation.RichsheetsConnection;
import rebound.richsheets.api.operation.RichsheetsOperation.RichsheetsOperationWithDataTimestamp;
import rebound.richsheets.api.operation.RichsheetsWriteData;
import rebound.richshets.model.cell.RichshetsCellContents;
import rebound.richshets.model.cell.RichshetsCellContents.RichshetsTextWrappingStrategy;
import javax.annotation.Nullable;

//TODO warnings observer!

/**
 * Note: Metatables don't exist at this level.  The code that handles them is a "client code" like any other as far as this is concerned! :D
 */
public class DesheeteningRichdatashetsConnection
implements RichdatashetsConnection
{
	protected RichsheetsConnection underlying;
	protected DesheeteningStrategy desheeteningStrategy;
	
	public DesheeteningRichdatashetsConnection(RichsheetsConnection underlying, DesheeteningStrategy desheeteningStrategy)
	{
		this.underlying = underlying;
		this.desheeteningStrategy = desheeteningStrategy;
	}
	
	
	public RichsheetsConnection getUnderlying()
	{
		return underlying;
	}
	
	public DesheeteningStrategy getDesheeteningStrategy()
	{
		return desheeteningStrategy;
	}
	
	
	@Override
	public Date getCurrentLastModifiedTimestamp() throws IOException
	{
		return underlying.getCurrentLastModifiedTimestamp();
	}
	
	
	
	
	
	//note: these can't be changed just as easily as changing this value!  there's things that rely on it other than that!
	private final static int descriptiveColumnNamesRowIndex = 0;
	private final static int semanticColumnUIDsRowIndex = 1;
	
	
	/**
	 * This is readonly if and only if performMaintenance = false and (operation = null or returns null).
	 * 
	 * + This does not throw an error if some of the multivalued columns in the provided map are missing from the richsheet, but it does throw an error in the converse (if some are in the sheet but we don't have an {@link RichdatashetsCellAbsenceStrategy absence strategy} for them!)
	 * 
	 * @param operation  if null, only validation and possibly maintenance are done, and a small amount of data is read (just metadata and the header rows).   to just validate the data, simply make this a non-null no-op that returns null :>
	 */
	@Override
	public void perform(boolean performMaintenance, RichdatashetsOperation operation) throws RichdatashetsStructureException, RichdatashetsUnencodableFormatException, IOException
	{
		boolean onlyReadHeaders = operation == null;
		
		underlying.perform(onlyReadHeaders ? 2 : null, new RichsheetsOperationWithDataTimestamp()
		{
			@Override
			public RichsheetsWriteData performInMemory(RichsheetsTable originalData, Date timestamp) throws RuntimeException
			{
				final boolean readonly;
				final int[][] columnInsertionIndexRangesInOperationSequenceOrder;  //values are {start, number}
				final Integer setFrozenRowsToThisOrDoNothingIfNull;
				final List<Integer> columnsToAutoResize;
				final List<RichsheetsRow> dataaaaaaaaaaaToWrite;
				final List<Integer> newColumnWidths;
				
				
				
				
				final int oldFrozenRowsCount = originalData.getFrozenRows();
				final int frozenColumnsCount = originalData.getFrozenColumns();
				
				
				
				//Take care of headers!
				int newFrozenRowsCount;
				if (performMaintenance)
				{
					if (oldFrozenRowsCount < 2)
					{
						setFrozenRowsToThisOrDoNothingIfNull = 2;
						newFrozenRowsCount = 2;
					}
					else
					{
						setFrozenRowsToThisOrDoNothingIfNull = null;
						newFrozenRowsCount = oldFrozenRowsCount;
					}
				}
				else
				{
					setFrozenRowsToThisOrDoNothingIfNull = null;
					newFrozenRowsCount = oldFrozenRowsCount;
				}
				
				
				
				if (originalData.getNumberOfRows() == 0)
				{
					throw new RichdatashetsStructureException("Spreadsheet is empty!");
				}
				else if (originalData.getNumberOfRows() == 1)
				{
					//Add in the semantic UIDs row
					if (performMaintenance)
					{
						dataaaaaaaaaaaToWrite = new ArrayList<>();
						dataaaaaaaaaaaToWrite.add(originalData.getRows().get(0));
						
						RichsheetsRow r = new RichsheetsRow();
						r.setCells(Collections.nCopies(originalData.getRows().get(0).getCells().size(), RichshetsCellContents.Blank));  //note that this handles frozen columns correctly implicitly :3
						dataaaaaaaaaaaToWrite.add(r);
						
						readonly = false;
						columnsToAutoResize = emptyList();
						newFrozenRowsCount = 2;
						columnInsertionIndexRangesInOperationSequenceOrder = new int[0][];
						newColumnWidths = originalData.getColumnWidths();
					}
					else
					{
						throw new RichdatashetsMaintenanceFixableStructureException("No Column Semantic UID row!");
					}
				}
				else
				{
					//This intermediate form is just like the original sheet but without the frozen columns
					final List<List<RichshetsCellContents>> dataDecoded = mapToList(row -> subListToEnd(row.getCells(), frozenColumnsCount), originalData.getRows());  //Remember: this is possibly just the header rows even if there are data rows!  (like an HTTP HEAD request! :D )
					final int numberOfRows = dataDecoded.size();  //Again, this might be only 2 for HEAD-style operations!
					final int numberOfColumns = originalData.getNumberOfColumns() - frozenColumnsCount;
					
					asrt(numberOfRows == originalData.getNumberOfRows());
					asrt(numberOfRows > 0);
					asrt(numberOfColumns == dataDecoded.get(0).size());
					
					
					
					//columnsToAutoResize
					{
						columnsToAutoResize = new ArrayList<>();
						
						for (int c = 0; c < numberOfColumns; c++)
						{
							@Nullable RichshetsCellContents d = originalData.getCell(c + frozenColumnsCount, semanticColumnUIDsRowIndex);
							
							boolean columnIsToBeAutoresized;
							{
								if (d != null)
								{
									boolean clip = d.getWrappingStrategy() == RichshetsTextWrappingStrategy.Clip;
									
									if (clip)
									{
										columnIsToBeAutoresized = false;
									}
									else
									{
										if (!d.isEmptyText())
										{
											columnIsToBeAutoresized = true;
										}
										else
										{
											//header uid cell is set to not clip and is empty
											@Nullable RichshetsCellContents d0 = originalData.getCell(c + frozenColumnsCount, descriptiveColumnNamesRowIndex);
											boolean descriptiveNameIsEmpty = d0 == null || d0.isEmptyText();
											
											columnIsToBeAutoresized = !descriptiveNameIsEmpty;
										}
									}
								}
								else
								{
									//header uid cell is set to not clip and is empty
									@Nullable RichshetsCellContents d0 = originalData.getCell(c + frozenColumnsCount, descriptiveColumnNamesRowIndex);
									boolean descriptiveNameIsEmpty = d0 == null || d0.isEmptyText();
									
									columnIsToBeAutoresized = !descriptiveNameIsEmpty;
								}
							}
							
							if (columnIsToBeAutoresized)
								columnsToAutoResize.add(c+frozenColumnsCount);
						}
					}
					
					
					
					final List<String> columnUIDs;  //empty strings means just that! it's preserved properly :3   //also, this contains the new autogenerated UIDs if any were to-do's!  //also this doesn't include frozen columns
					final Map<String, List<Integer>> columnIndexesByUID;  //Indexes here start with the first unfrozen column as "0", they aren't the google sheets column indexes!
					{
						columnUIDs = new ArrayList<>();
						columnIndexesByUID = new HashMap<>();
						
						for (int c = 0; c < numberOfColumns; c++)
						{
							String uid = dataDecoded.get(semanticColumnUIDsRowIndex).get(c).justText();  //row index 1 is always the semantic UIDs row!  (row index 0 is always the descriptive names row)
							
							uid = uid.trim();
							
							boolean skip;
							
							if (isTodoUIDValue(uid))
							{
								if (performMaintenance)
								{
									uid = autogenerateUID().toUpperCase();
									skip = false;
								}
								else
								{
									//TODO warn
									uid = "";
									skip = true;
								}
							}
							else
							{
								skip = uid.isEmpty();
								
								if (!skip)
									uid = uid.toUpperCase();
							}
							
							
							columnUIDs.add(uid);
							
							if (!skip)
							{
								List<Integer> l = columnIndexesByUID.get(uid);
								
								if (l == null)
								{
									l = new ArrayList<>();
									columnIndexesByUID.put(uid, l);
								}
								
								l.add(c);
							}
						}
					}
					
					
					
					final Set<String> singleValueColumnUIDs;
					final Set<String> multiValueColumnUIDs;
					{
						singleValueColumnUIDs = new HashSet<>();
						multiValueColumnUIDs = new HashSet<>();
						
						for (Entry<String, List<Integer>> e : columnIndexesByUID.entrySet())
							(e.getValue().size() > 1 ? multiValueColumnUIDs : singleValueColumnUIDs).add(e.getKey());
						
						
						
						Set<String> multiValueColumnUIDsInSheetButNotInProvided;
						{
							multiValueColumnUIDsInSheetButNotInProvided = new HashSet<>(multiValueColumnUIDs);
							multiValueColumnUIDsInSheetButNotInProvided.removeAll(desheeteningStrategy.getMultivalueColumnsAbsenceStrategies().keySet());
						}
						
						if (!multiValueColumnUIDsInSheetButNotInProvided.isEmpty())
						{
							throw new RichdatashetsStructureException("These column UIDs were duplicated but weren't listed as supported multivalued columns: "+multiValueColumnUIDsInSheetButNotInProvided);
						}
						
						//We'll say it's fine if the sheet is missing a multivalued column that's given (since we allow that for single-value columns!  maybe the user wants the code to work on many tables and work for a superset of columns (all possible columns) and some are optional *cough* like metatables!?!! *cough* XD )
					}
					
					
					
					
					
					
					List<List<RichshetsCellContents>> newNonSemanticHeaderRowsCells;
					int newNumberOfColumns;
					final List<String> newColumnUIDs;  //empty strings means just that! it's preserved properly :3   //also, this contains the new autogenerated UIDs if any were to-do's!  //also this doesn't include frozen columns
					final Map<String, List<Integer>> newColumnIndexesByUID;  //Indexes here start with the first unfrozen column as "0", they aren't the google sheets column indexes!
					List<Integer> oldIntermediateColumnIndexesGivenNewOverloaded;  //indexes are new ones, values are old ones; if there isn't an old one, the one that was immediately before it is before it was inserted is used :3
					//int[] newIntermediateColumnIndexesGivenOld;  //indexes are old ones, values are new ones
					List<RichsheetsRow> newDataRows;  //like a new version of the intermediate data (intermediate form) except "data rows" meaning no header rows!  (empty list means erase all data, null means don't alter post-header rows!)
					
					if (operation != null)
					{
						RichdatashetsTable decoded;
						{
							//no need to sort; the Datashets Column Indexes are completely undefined, arbitrary, meaningless, and and not guaranteed to be the same across sessions; that's why we have UIDs X3
							RichdatashetsSemanticColumns singleValueColumns = new RichdatashetsSemanticColumns(new ArrayList<>(singleValueColumnUIDs));
							RichdatashetsSemanticColumns multiValueColumns = new RichdatashetsSemanticColumns(new ArrayList<>(multiValueColumnUIDs));
							
							
							//indexes here match with decoded form, values are indexes into intermediate form
							int[] intermediateColumnIndexesForEachDecodedSingleValueColumn;
							{
								int n = singleValueColumns.size();
								
								intermediateColumnIndexesForEachDecodedSingleValueColumn = new int[n];
								
								for (int i = 0; i < n; i++)
								{
									String uid = singleValueColumns.getUIDByIndex(i);
									
									List<Integer> l = columnIndexesByUID.get(uid);
									if (l.size() != 1)  throw new AssertionError();  //this can't happen XD'
									
									intermediateColumnIndexesForEachDecodedSingleValueColumn[i] = l.get(0);
								}
							}
							
							
							//indexes here match with decoded form, values' values are indexes into intermediate form
							int[][] intermediateColumnIndexesForEachDecodedMultiValueColumn;
							RichdatashetsCellAbsenceStrategy[] absenceStrategiesForEachDecodedMultiValueColumn;
							{
								int n = multiValueColumns.size();
								
								intermediateColumnIndexesForEachDecodedMultiValueColumn = new int[n][];
								absenceStrategiesForEachDecodedMultiValueColumn = new RichdatashetsCellAbsenceStrategy[n];
								
								for (int i = 0; i < n; i++)
								{
									String uid = multiValueColumns.getUIDByIndex(i);
									
									List<Integer> l = columnIndexesByUID.get(uid);
									if (l.isEmpty())  throw new AssertionError();  //this can't happen XD'
									
									int[] a = new int[l.size()];
									for (int j = 0; j < a.length; j++)
										a[j] = l.get(j);
									
									Arrays.sort(a);  //preserve order from spreadsheet for multivalued column list-values (the only thing from the original spreadsheet we preserve the order of X3 )
									
									RichdatashetsCellAbsenceStrategy as = desheeteningStrategy.getMultivalueColumnsAbsenceStrategies().get(uid);
									if (as == null)  throw new AssertionError();  //we checked for this earlier!
									
									intermediateColumnIndexesForEachDecodedMultiValueColumn[i] = a;
									absenceStrategiesForEachDecodedMultiValueColumn[i] = as;
								}
							}
							
							
							
							
							
							List<RichdatashetsRow> rows;
							{
								rows = new ArrayList<>();
								
								for (int r = newFrozenRowsCount; r < numberOfRows; r++)
								{
									List<RichshetsCellContents> intermediateRow = dataDecoded.get(r);
									
									RichshetsCellContents[] singlesInDecodedOrder;
									{
										int n = intermediateColumnIndexesForEachDecodedSingleValueColumn.length;
										singlesInDecodedOrder = new RichshetsCellContents[n];
										
										for (int i = 0; i < n; i++)
											singlesInDecodedOrder[i] = intermediateRow.get(intermediateColumnIndexesForEachDecodedSingleValueColumn[i]);
									}
									
									List<RichshetsCellContents>[] multisInDecodedOrder;
									{
										int n = intermediateColumnIndexesForEachDecodedMultiValueColumn.length;
										multisInDecodedOrder = new List[n];
										
										for (int i = 0; i < n; i++)
										{
											RichdatashetsCellAbsenceStrategy as = absenceStrategiesForEachDecodedMultiValueColumn[i];
											
											List<RichshetsCellContents> l = new ArrayList<>();  //has to be mutable for the client code!
											{
												for (int c : intermediateColumnIndexesForEachDecodedMultiValueColumn[i])
												{
													RichshetsCellContents v = intermediateRow.get(c);
													
													if (!as.isAbsent(v))
														l.add(v);  //preserve order!
												}
											}
											
											multisInDecodedOrder[i] = l;
										}
									}
									
									RichdatashetsRow datashetRow = new RichdatashetsRow(asList(singlesInDecodedOrder), asList(multisInDecodedOrder));
									
									rows.add(datashetRow);
								}
							}
							
							decoded = new RichdatashetsTable(singleValueColumns, multiValueColumns, rows);
						}
						
						
						
						
						RichdatashetsTable toWrite = operation instanceof RichdatashetsOperationWithDataTimestamp ? ((RichdatashetsOperationWithDataTimestamp)operation).performInMemory(decoded, timestamp) : operation.performInMemory(decoded);
						
						readonly = toWrite == null && !performMaintenance;
						
						
						
						//toWrite → newDataRows
						if (toWrite != null)
						{
							//Note that we don't require exact sameness, only uid-set sameness not the order!  regenerate the column mappings if it's different!
							if (!toWrite.getColumnsSingleValued().hasSameUIDsIgnoringOrder(decoded.getColumnsSingleValued()))
								throw new IllegalArgumentException("Columns must be preserved during a Datashets Client-Code Operation!!  (Rows can be added or removed, but not columns!)");
							
							if (!toWrite.getColumnsMultiValued().hasSameUIDsIgnoringOrder(decoded.getColumnsMultiValued()))
								throw new IllegalArgumentException("Columns must be preserved during a Datashets Client-Code Operation!!  (Rows can be added or removed, but not columns!)");
							
							
							
							int nRows = toWrite.getNumberOfRows();
							
							
							
							//Calculate columns we need to add!
							{
								int n = toWrite.getNumberOfColumnsMultiValued();
								
								Map<String, Integer> numberOfMultivalueColumnsToAddForEachMultilinePseudocolumn = new HashMap<>();
								{
									for (int c = 0; c < n; c++)
									{
										String uid = toWrite.getColumnsMultiValued().getUIDByIndex(c);
										
										int max = 0;  //a fine "no rows" value X3
										
										for (int r = 0; r < nRows; r++)
										{
											List<RichshetsCellContents> l = toWrite.getMultiCell(c, r);
											int ln = l.size();
											
											if (ln > max)
												max = ln;
										}
										
										int prev = columnIndexesByUID.get(uid).size();
										if (max > prev)
										{
											if (numberOfMultivalueColumnsToAddForEachMultilinePseudocolumn.put(uid, max - prev) != null)  throw new AssertionError();
										}
									}
								}
								
								
								
								//newColumnUIDs
								//newColumnWidths
								//newDescriptiveRowCells
								//columnInsertionIndexRangesInOperationSequenceOrder
								//oldIntermediateColumnIndexesGivenNewOverloaded
								{
									newColumnUIDs = new ArrayList<>(columnUIDs);
									newColumnWidths = new ArrayList<>(originalData.getColumnWidths());
									
									newNonSemanticHeaderRowsCells = new ArrayList<>(newFrozenRowsCount - 1);  //minus the one semantic header row
									{
										int nr = newFrozenRowsCount - 1;  //minus the one semantic header row
										newNonSemanticHeaderRowsCells = new ArrayList<>(nr);
										
										for (int r = 0; r < nr; r++)
											newNonSemanticHeaderRowsCells.set(r, new ArrayList<>(originalData.getRows().get(r == 0 ? descriptiveColumnNamesRowIndex : r - 1).getCells()));
									}
									
									Set<String> uidsNeedingIt = numberOfMultivalueColumnsToAddForEachMultilinePseudocolumn.keySet();
									
									String[] uidsNeedingItInReverseOrderOfLastColumn = uidsNeedingIt.toArray(new String[uidsNeedingIt.size()]);
									Arrays.sort(uidsNeedingItInReverseOrderOfLastColumn, (a, b) ->
									{
										List<Integer> idsA = columnIndexesByUID.get(a);
										int lastA = idsA.get(idsA.size() - 1);
										
										List<Integer> idsB = columnIndexesByUID.get(b);
										int lastB = idsB.get(idsB.size() - 1);
										
										return lastA > lastB ? -1 : (lastA == lastB ? 0 : 1);  //reverse order :3
									});
									
									
									
									oldIntermediateColumnIndexesGivenNewOverloaded = new ArrayList<>(numberOfColumns);
									for (int i = 0; i < numberOfColumns; i++)
										oldIntermediateColumnIndexesGivenNewOverloaded.add(i);
									
									
									int nc = uidsNeedingItInReverseOrderOfLastColumn.length;
									columnInsertionIndexRangesInOperationSequenceOrder = new int[nc][2];
									{
										for (int c = 0; c < nc; c++)
										{
											String uid = uidsNeedingItInReverseOrderOfLastColumn[c];
											
											List<Integer> ids = columnIndexesByUID.get(uid);
											int last = ids.get(ids.size() - 1);
											
											int insertionPoint = last + 1;
											int numberToInsert = numberOfMultivalueColumnsToAddForEachMultilinePseudocolumn.get(uid);
											
											int[] a = columnInsertionIndexRangesInOperationSequenceOrder[c];
											a[0] = insertionPoint;
											a[1] = numberToInsert;
											
											if (insertionPoint == 0)  throw new AssertionError();  //we always insert after columns and we copy the data from the one before (left) not after (right)!
											int previousColumnOldIndex = insertionPoint - 1;
											
											Integer width = originalData.getColumnWidth(previousColumnOldIndex);
											
											//Copy UID and width
											for (int i = 0; i < numberToInsert; i++)
											{
												newColumnUIDs.add(insertionPoint, uid);
												newColumnWidths.add(insertionPoint, width);
											}
											
											//Copy other header rows stuff
											int nr = newFrozenRowsCount - 1;  //minus the one semantic header row
											for (int r = 0; r < nr; r++)
											{
												List<RichshetsCellContents> row = newNonSemanticHeaderRowsCells.get(r);
												
												RichshetsCellContents v = row.get(previousColumnOldIndex);
												for (int i = 0; i < numberToInsert; i++)
													row.add(insertionPoint, v);
											}
											
											//Copy index of column to use as a template for other things
											for (int i = 0; i < numberToInsert; i++)
												oldIntermediateColumnIndexesGivenNewOverloaded.add(insertionPoint, previousColumnOldIndex);
										}
									}
									
									
									newNumberOfColumns = newColumnUIDs.size();
									
									
									//Todo merge adjacent intervals in a pass here for performance optimization!
									//columnInsertionIndexRangesInOperationSequenceOrder = mergeReverseAdjacentIntervals(columnInsertionIndexRangesInOperationSequenceOrder);
								}
								
								
								
								
								//newColumnIndexesByUID
								{
									//Just invert newColumnUIDs  :3
									
									newColumnIndexesByUID = new HashMap<>();
									
									for (int c = 0; c < newNumberOfColumns; c++)
									{
										String uid = newColumnUIDs.get(c);
										List<Integer> l = newColumnIndexesByUID.get(uid);
										
										if (l == null)
										{
											l = new ArrayList<>();
											newColumnIndexesByUID.put(uid, l);
										}
										
										l.add(c);
									}
								}
							}
							
							
							
							
							
							/*
							 * Be careful to use New Column Indexes as needed after this point!!
							 * 
							 * Specifically, vet uses of:
							 * 		numberOfColumns
							 * 		columnUIDs
							 * 		columnIndexesByUID
							 * 		originalData
							 * 		dataDecoded
							 */
							
							
							
							
							newDataRows = new ArrayList<>(nRows);
							{
								RichdatashetsSemanticColumns singleValueColumns = toWrite.getColumnsSingleValued();
								RichdatashetsSemanticColumns multiValueColumns = toWrite.getColumnsMultiValued();
								
								
								//indexes here match with decoded form, values are indexes into intermediate form
								int[] newIntermediateColumnIndexesForEachDecodedSingleValueColumn;
								{
									int n = singleValueColumns.size();
									
									newIntermediateColumnIndexesForEachDecodedSingleValueColumn = new int[n];
									
									for (int i = 0; i < n; i++)  //i is the made-up column index in the client code's output!
									{
										String uid = singleValueColumns.getUIDByIndex(i);
										
										List<Integer> l = newColumnIndexesByUID.get(uid);
										if (l.size() != 1)  throw new AssertionError();  //this can't happen XD'
										
										newIntermediateColumnIndexesForEachDecodedSingleValueColumn[i] = l.get(0);
									}
								}
								
								
								//indexes here match with decoded form, values' values are indexes into intermediate form
								int[][] newIntermediateColumnIndexesForEachDecodedMultiValueColumn;
								RichdatashetsCellAbsenceStrategy[] absenceStrategiesForEachDecodedMultiValueColumn;
								{
									int n = multiValueColumns.size();
									
									newIntermediateColumnIndexesForEachDecodedMultiValueColumn = new int[n][];
									absenceStrategiesForEachDecodedMultiValueColumn = new RichdatashetsCellAbsenceStrategy[n];
									
									for (int i = 0; i < n; i++)  //i is the made-up column index in the client code's output!
									{
										String uid = multiValueColumns.getUIDByIndex(i);
										
										List<Integer> l = newColumnIndexesByUID.get(uid);
										if (l.isEmpty())  throw new AssertionError();  //this can't happen XD'
										
										int[] a = new int[l.size()];
										for (int j = 0; j < a.length; j++)
											a[j] = l.get(j);
										
										Arrays.sort(a);  //preserve order from spreadsheet for multivalued column list-values (the only thing from the original spreadsheet we preserve the order of X3 )
										
										RichdatashetsCellAbsenceStrategy as = desheeteningStrategy.getMultivalueColumnsAbsenceStrategies().get(uid);
										if (as == null)  throw new AssertionError();  //we checked for this earlier!
										
										newIntermediateColumnIndexesForEachDecodedMultiValueColumn[i] = a;
										absenceStrategiesForEachDecodedMultiValueColumn[i] = as;
									}
								}
								
								
								
								
								for (int rowIndex = 0; rowIndex < nRows; rowIndex++)
								{
									RichdatashetsRow ourRow = toWrite.getRows().get(rowIndex);
									
									int originalDataRowIndex = ourRow.getOriginalDataRowIndex();
									
									RichshetsCellContents[] newIntermediateRowA;
									Integer newRowHeight;
									{
										if (originalDataRowIndex == -1)
										{
											newIntermediateRowA = new RichshetsCellContents[newNumberOfColumns];
											Arrays.fill(newIntermediateRowA, RichshetsCellContents.Blank);
											newRowHeight = null;
										}
										else if (originalDataRowIndex < 0)
										{
											throw new IllegalArgumentException("originalDataRowIndex must just be -1 for the special 'new row' effect, not -2 or beyond X3");
										}
										else
										{
											if (originalDataRowIndex >= numberOfRows - newFrozenRowsCount)
												throw new IllegalArgumentException("originalDataRowIndex was too large; it was "+originalDataRowIndex+" but there were only "+(numberOfRows - newFrozenRowsCount)+" data rows in the spreadsheet!  (remember, all indexes here are zero-based X3 )");
											
											if (dataDecoded.size() != numberOfRows)  throw new AssertionError();
											List<RichshetsCellContents> oldRow = new ArrayList<>(dataDecoded.get(oldFrozenRowsCount + originalDataRowIndex));
											newRowHeight = originalData.getRows().get(oldFrozenRowsCount + originalDataRowIndex).getHeight();
											if (oldRow.size() != numberOfColumns)  throw new AssertionError();
											
											//Apply new column insertions to it!
											applyColumnInsertions(oldRow, columnInsertionIndexRangesInOperationSequenceOrder, null);  //null because they'll be overwritten by the multivalue setting one :3
											
											newIntermediateRowA = oldRow.toArray(new RichshetsCellContents[newNumberOfColumns]);
										}
									}
									
									
									//Singles
									{
										int n = newIntermediateColumnIndexesForEachDecodedSingleValueColumn.length;
										for (int i = 0; i < n; i++)
											newIntermediateRowA[newIntermediateColumnIndexesForEachDecodedSingleValueColumn[i]] = toWrite.getCell(i, rowIndex);
									}
									
									
									//Multi's
									{
										int n = newIntermediateColumnIndexesForEachDecodedMultiValueColumn.length;
										for (int i = 0; i < n; i++)
										{
											RichdatashetsCellAbsenceStrategy as = absenceStrategiesForEachDecodedMultiValueColumn[i];
											List<RichshetsCellContents> l = toWrite.getMultiCell(i, rowIndex);
											
											int[] intermediateColumnIndexes = newIntermediateColumnIndexesForEachDecodedMultiValueColumn[i];
											
											int nl = l.size();
											int nc = intermediateColumnIndexes.length;
											int j = 0;
											
											//Write out the cells we have
											for (; j < nl; j++)
												newIntermediateRowA[intermediateColumnIndexes[j]] = l.get(j);
											
											//Then fill in blanks (or whatever "absent" is encoded as) for the ones we don't!
											for (; j < nc; j++)
												newIntermediateRowA[intermediateColumnIndexes[j]] = as.getAbsentValueForNewCells();
										}
									}
									
									
									//Actually add it! XD :D
									newDataRows.add(new RichsheetsRow(asList(newIntermediateRowA), newRowHeight));
								}
							}
						}
						else
						{
							newDataRows = null;
							columnInsertionIndexRangesInOperationSequenceOrder = new int[0][];
							newColumnUIDs = columnUIDs;
							newColumnWidths = originalData.getColumnWidths();
							newColumnIndexesByUID = columnIndexesByUID;
							newNumberOfColumns = numberOfColumns;
							oldIntermediateColumnIndexesGivenNewOverloaded = null;
							
							newNonSemanticHeaderRowsCells = new ArrayList<>(newFrozenRowsCount - 1);
							{
								newNonSemanticHeaderRowsCells.add(originalData.getRows().get(descriptiveColumnNamesRowIndex).getCells());
								
								for (int r = 2; r < newFrozenRowsCount; r++)
									newNonSemanticHeaderRowsCells.add(originalData.getRows().get(r - 2 + 1).getCells());
							}
						}
					}
					else
					{
						readonly = !performMaintenance;
						
						newDataRows = null;
						columnInsertionIndexRangesInOperationSequenceOrder = new int[0][];
						newColumnUIDs = columnUIDs;
						newColumnWidths = originalData.getColumnWidths();
						newColumnIndexesByUID = columnIndexesByUID;
						newNumberOfColumns = numberOfColumns;
						oldIntermediateColumnIndexesGivenNewOverloaded = null;
						
						newNonSemanticHeaderRowsCells = new ArrayList<>(newFrozenRowsCount - 1);
						{
							newNonSemanticHeaderRowsCells.add(originalData.getRows().get(descriptiveColumnNamesRowIndex).getCells());
							
							for (int r = 2; r < newFrozenRowsCount; r++)
								newNonSemanticHeaderRowsCells.add(originalData.getRows().get(r - 2 + 1).getCells());
						}
						
						//TODO more? X3
					}
					
					
					
					
					
					
					
					
					if (readonly)
					{
						dataaaaaaaaaaaToWrite = null;
					}
					else
					{
						//Headers + newDataRows → dataaaaaaaaaaa
						dataaaaaaaaaaaToWrite = new ArrayList<>();
						{
							//Header rows
							{
								dataaaaaaaaaaaToWrite.add(new RichsheetsRow(newNonSemanticHeaderRowsCells.get(0), originalData.getNumberOfRows() > descriptiveColumnNamesRowIndex ? originalData.getRows().get(descriptiveColumnNamesRowIndex).getHeight() : null));
								
								RichsheetsRow semanticHeadersRow;
								{
									List<RichshetsCellContents> semanticHeadersRowCells;
									{
										semanticHeadersRowCells = new ArrayList<>();
										
										//Frozen columns at the start are unaffected by our column additions :3
										for (int columnIndexInRichsheet = 0; columnIndexInRichsheet < frozenColumnsCount; columnIndexInRichsheet++)
										{
											RichshetsCellContents cell = originalData.getCell(columnIndexInRichsheet, semanticColumnUIDsRowIndex);
											
											if (cell == null)
												cell = RichshetsCellContents.Blank;  //header cells are never proper booleans
											
											semanticHeadersRowCells.add(cell);
										}
										
										for (int newColumnIndexInIntermediate = 0; newColumnIndexInIntermediate < newNumberOfColumns; newColumnIndexInIntermediate++)
										{
											RichshetsCellContents cell = dataDecoded.get(semanticColumnUIDsRowIndex).get(oldIntermediateColumnIndexesGivenNewOverloaded == null ? newColumnIndexInIntermediate : oldIntermediateColumnIndexesGivenNewOverloaded.get(newColumnIndexInIntermediate));
											
											cell = cell.withOtherText(newColumnUIDs.get(newColumnIndexInIntermediate));
											
											semanticHeadersRowCells.add(cell);  //header cells are never proper booleans
										}
									}
									
									semanticHeadersRow = new RichsheetsRow(semanticHeadersRowCells, originalData.getNumberOfRows() > semanticColumnUIDsRowIndex ? originalData.getRows().get(semanticColumnUIDsRowIndex).getHeight() : null);
									
									dataaaaaaaaaaaToWrite.add(semanticHeadersRow);
								}
								
								
								//We don't change any of the other frozen rows either!
								//Except to accommodate our column insertions!
								for (int rowIndexInRichsheet = 2; rowIndexInRichsheet < newFrozenRowsCount; rowIndexInRichsheet++)
									dataaaaaaaaaaaToWrite.add(new RichsheetsRow(newNonSemanticHeaderRowsCells.get(rowIndexInRichsheet - 2 + 1), originalData.getNumberOfRows() > rowIndexInRichsheet ? originalData.getRows().get(rowIndexInRichsheet).getHeight() : null));
							}
							
							
							//Data rows!
							{
								if (newDataRows == null)
								{
									//If this is null, there will never be any column insertions, so don't worry about that! :D
									//TODO make sure this is valid XD'
									if (columnInsertionIndexRangesInOperationSequenceOrder.length > 0)  throw new AssertionError();
									
									dataaaaaaaaaaaToWrite.addAll(originalData.getRows().subList(newFrozenRowsCount > originalData.getNumberOfRows() ? originalData.getNumberOfRows() : newFrozenRowsCount, originalData.getNumberOfRows()));
								}
								else
								{
									int n = newDataRows.size();
									for (int rowIndexAfterHeaderRows = 0; rowIndexAfterHeaderRows < n; rowIndexAfterHeaderRows++)
									{
										RichsheetsRow ourRow = newDataRows.get(rowIndexAfterHeaderRows);
										
										List<RichshetsCellContents> theirRowCells = new ArrayList<>(ourRow.getCells().size());
										
										//Frozen columns at the start are unaffected by our column additions :3
										for (int columnIndexInRichsheet = 0; columnIndexInRichsheet < frozenColumnsCount; columnIndexInRichsheet++)
										{
											int rowIndexInRichsheet = newFrozenRowsCount + rowIndexAfterHeaderRows;
											
											RichshetsCellContents cell = originalData.getCell(columnIndexInRichsheet, rowIndexInRichsheet);
											
											if (cell == null)
												cell = RichshetsCellContents.Blank;  //header cells are never proper booleans
											
											theirRowCells.add(cell);
										}
										
										for (int newColumnIndexInIntermediate = 0; newColumnIndexInIntermediate < newNumberOfColumns; newColumnIndexInIntermediate++)
										{
											RichshetsCellContents cell = ourRow.getCells().get(newColumnIndexInIntermediate);
											
											theirRowCells.add(cell);
										}
										
										RichsheetsRow theirRow = new RichsheetsRow(theirRowCells, ourRow.getHeight());
										
										dataaaaaaaaaaaToWrite.add(theirRow);
									}
								}
							}
						}
					}
				}
				
				
				
				
				
				RichsheetsTable outputTable = new RichsheetsTable(dataaaaaaaaaaaToWrite);
				
				outputTable.setFrozenColumns(frozenColumnsCount);
				
				if (setFrozenRowsToThisOrDoNothingIfNull != null)
					outputTable.setFrozenRows(setFrozenRowsToThisOrDoNothingIfNull);
				
				
				outputTable.setColumnWidths(newColumnWidths);
				
				
				return new RichsheetsWriteData(outputTable, asSetThrowing(columnsToAutoResize));
			}
		});
	}
	
	
	
	//Todo add to out Testing Datashets! XD
	public static boolean isTodoUIDValue(String t)
	{
		t = t.trim();
		
		while (t.endsWith("!") || t.endsWith("."))
			t = t.substring(0, t.length()-1);
		
		return t.equalsIgnoreCase("todo") || t.equalsIgnoreCase("to-do") || t.equalsIgnoreCase("to do") || t.equalsIgnoreCase("tbd");
	}
	
	
	protected Random random = new Random();
	public String autogenerateUID()
	{
		int i = random.nextInt();
		String s = Integer.toHexString(i).toUpperCase();
		return mulnn('0', 8 - s.length()) + s;
	}
	
	
	
	
	protected static <E> void applyColumnInsertions(List<E> listToInsertInto, int[][] columnInsertionIndexRangesInOperationSequenceOrder, E valueToInsert)
	{
		for (int[] i : columnInsertionIndexRangesInOperationSequenceOrder)
		{
			int insertionPoint = i[0];
			int number = i[1];
			
			for (int j = 0; j < number; j++)
				listToInsertInto.add(insertionPoint, valueToInsert);
		}
	}
}
