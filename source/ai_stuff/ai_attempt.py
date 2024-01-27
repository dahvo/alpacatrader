import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import TimeSeriesSplit
from sklearn.ensemble import RandomForestClassifier
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from sklearn.metrics import accuracy_score

# Load your data
df = pd.read_csv('data/bars/AAPL_6mo_hr.csv')

# Convert timestamp to datetime and set it as index
df['timestamp'] = pd.to_datetime(df['timestamp'])
df.set_index('timestamp', inplace=True)

# Resample data to daily
agg_dict = {
    'open': 'first',
    'high': 'max',
    'low': 'min',
    'close': 'last',
    'volume': 'sum',
    'trade_count': 'sum',
    'vwap': 'mean'  # This is a simplification, normally VWAP needs weighted averaging
}
daily_df = df.resample('D').agg(agg_dict).dropna()

# Feature Engineering
# Example: Add moving average
daily_df['moving_avg_10'] = daily_df['close'].rolling(window=10).mean()

# Target Variable
daily_df['Target'] = np.where(daily_df['close'].shift(-1) > daily_df['close'], 1, 0)

# Select features
features = ['open', 'high', 'low', 'close', 'volume', 'trade_count', 'vwap', 'moving_avg_10']
X = daily_df[features]
y = daily_df['Target']

# Normalize the features
scaler = MinMaxScaler()
X_scaled = scaler.fit_transform(X)

# Splitting into training and testing sets using TimeSeriesSplit
tscv = TimeSeriesSplit(n_splits=10)
for train_index, test_index in tscv.split(X_scaled):
    X_train, X_test = X_scaled[train_index], X_scaled[test_index]
    y_train, y_test = y.iloc[train_index], y.iloc[test_index]

# Reshape input for LSTM
X_train = X_train.reshape((X_train.shape[0], 1, X_train.shape[1]))
X_test = X_test.reshape((X_test.shape[0], 1, X_test.shape[1]))

# LSTM Model
lstm_model = Sequential()
lstm_model.add(LSTM(units=50, return_sequences=True, input_shape=(X_train.shape[1], X_train.shape[2])))
lstm_model.add(LSTM(units=50))
lstm_model.add(Dense(1))

lstm_model.compile(loss='mean_squared_error', optimizer='adam')
lstm_model.fit(X_train, y_train, epochs=100, batch_size=32, verbose=1, shuffle=False)

# Random Forest Model
rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
rf_model.fit(X_train.reshape(X_train.shape[0], X_train.shape[2]), y_train)

# Making predictions
lstm_predictions = lstm_model.predict(X_test)
rf_predictions = rf_model.predict(X_test.reshape(X_test.shape[0], X_test.shape[2]))

# Evaluating models
lstm_accuracy = accuracy_score(y_test, np.round(lstm_predictions))
rf_accuracy = accuracy_score(y_test, rf_predictions)

print(f"LSTM Model Accuracy: {lstm_accuracy}")
print(f"Random Forest Model Accuracy: {rf_accuracy}")
