package org.wkh.bateman.fetch;

import java.math.BigDecimal;
import java.util.ArrayList;
import org.joda.time.DateTime;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wkh.bateman.pso.SimpleParticleSwarmOptimizer;
import org.wkh.bateman.trade.TimeSeries;

public class YahooQuoteFetcher extends QuoteFetcher {
	private static Logger logger = LoggerFactory.getLogger(YahooQuoteFetcher.class.getName());

    public TimeSeries fetchAndParseDaily(String symbol, int days) throws Exception {
        return fetchAndParse(symbol, days, 60 * 60 * 24);
    }

    public BigDecimal fetchBidAskSpread(String symbol) throws Exception {
        String url = "http://download.finance.yahoo.com/d/quotes.csv?s=" + symbol + "&f=b2b3";
    	//String url = "http://finance.yahoo.com/d/quotes.csv?s=" + symbol + "&f=b2b3";
    	System.out.println(url);
        
        String result = fetchURLasString(url).replaceAll("\r\n", "").replaceAll("\n", "");
        System.out.println(result);
        String[] parts = result.split(",");

        return new BigDecimal(parts[0]).subtract(new BigDecimal(parts[1]));
    }

    @Override
    public String fetchQuotes(String symbol, int days, int interval) throws Exception {
        String period;
        String quotes = null;
        switch (interval) {
            case 60 * 60 * 24:
                period = "d";
                break;
            case 60 * 60 * 24 * 7:
                period = "w";
                break;
            default:
                throw new Exception();
        }

        DateTime now = new DateTime();
        DateTime startDate = now.minusDays(days);

        int endMonth = now.getMonthOfYear() - 1;
        int endDay = now.getDayOfMonth();
        int endYear = now.getYear();

        int startMonth = startDate.getMonthOfYear() - 1;
        int startDay = startDate.getDayOfMonth();
        int startYear = startDate.getYear();
        
        //   Date, Open, High, Low, Close, Volume, Adj Close
        String url = String.format("http://ichart.yahoo.com/table.csv?s=%s&a=%d&b=%d&c=%d&d=%d&e=%d&f=%d&g=%s&ignore=.csv",
                symbol, startMonth, startDay, startYear, endMonth, endDay, endYear, period);
        
        System.out.println(url);
        quotes = fetchURLasString(url);
       
        logger.debug(quotes);
        return quotes;
    }

    @Override
    public List<Quote> parseQuotes(String quoteList, int interval) {
        List<Quote> quotes = new ArrayList<Quote>();

        String[] lines = dropLines(quoteList, 1);

        for (String line : lines) {
            String[] parts = line.split(",");

            // Date,Open,High,Low,Close,Volume,Adj Close
            DateTime date = DateTime.parse(parts[0]);

            Quote quote = new Quote(date,
                    interval,
                    new BigDecimal(parts[1]),
                    new BigDecimal(parts[2]),
                    new BigDecimal(parts[3]),
                    new BigDecimal(parts[6]),
                    Integer.parseInt(parts[5]));

            quotes.add(quote);
        }

        return quotes;
    }

    public static void main(String[] args) throws Exception {
        YahooQuoteFetcher fetcher = new YahooQuoteFetcher();
        TimeSeries series = fetcher.fetchAndParseDaily("AAPL", 5);

        for (Map.Entry<DateTime, BigDecimal> entry : series.getPrices().entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }

        System.out.println(fetcher.fetchBidAskSpread("AAPL"));
    }
}
