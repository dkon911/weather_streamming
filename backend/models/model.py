import joblib

with open(r'C:\Users\Dell\PycharmProjects\data_engineering\notebook\best_model.pkl', 'rb') as f:
    model = joblib.load(f)
