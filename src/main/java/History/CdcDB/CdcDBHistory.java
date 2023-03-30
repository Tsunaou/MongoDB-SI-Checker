package History.CdcDB;

import Exceptions.HistoryInvalidException;
import History.History;

import java.util.ArrayList;

public class CdcDBHistory extends History<CdcDBTransaction> {
    public CdcDBHistory(ArrayList<CdcDBTransaction> transactions) throws HistoryInvalidException {
        super(transactions);
    }


}
