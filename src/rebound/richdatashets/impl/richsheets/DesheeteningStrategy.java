package rebound.richdatashets.impl.richsheets;

import java.util.Map;
import rebound.richdatashets.api.model.RichdatashetsCellAbsenceStrategy;

public class DesheeteningStrategy
{
	protected final Map<String, RichdatashetsCellAbsenceStrategy> multivalueColumnsAbsenceStrategies;
	
	public DesheeteningStrategy(Map<String, RichdatashetsCellAbsenceStrategy> multivalueColumnsAbsenceStrategies)
	{
		this.multivalueColumnsAbsenceStrategies = multivalueColumnsAbsenceStrategies;
	}
	
	public Map<String, RichdatashetsCellAbsenceStrategy> getMultivalueColumnsAbsenceStrategies()
	{
		return multivalueColumnsAbsenceStrategies;
	}
}
