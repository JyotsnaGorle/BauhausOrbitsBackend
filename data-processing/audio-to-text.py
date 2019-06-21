import os
import glob
from os import path
import speech_recognition as sr
from pydub import AudioSegment

GOOGLE_SPEECH_KEY = "AIzaSyC0T0dWnAqW6AEoYw0aeXV7U5ZHKpTGyME"

for audio_file_name in glob.glob('sounds/*.mp3'):

    sound = AudioSegment.from_mp3(audio_file_name)
    
    new_audio_file_name = audio_file_name.replace(".mp3", ".wav")
    
    if not os.path.exists(new_audio_file_name):
        sound.export(new_audio_file_name, format="wav")

    # use the audio file as the audio source
    r = sr.Recognizer()
    
    with sr.AudioFile(new_audio_file_name) as source:
        audio = r.record(source)

        # recognize speech using Sphinx
        # try:
            # print("Sphinx thinks you said " + r.recognize_sphinx(audio))
        # except sr.UnknownValueError:
            # print("Sphinx could not understand audio")
        # except sr.RequestError as e:
            # print("Sphinx error; {0}".format(e))

        # recognize speech using Google Speech Recognition
        # try:
            # print("Google Speech Recognition thinks you said " + r.recognize_google(audio, key="AIzaSyC0T0dWnAqW6AEoYw0aeXV7U5ZHKpTGyME"))
        # except sr.UnknownValueError:
            # print("Google Speech Recognition could not understand audio")
        # except sr.RequestError as e:
            # print("Could not request results from Google Speech Recognition service; {0}".format(e))
            
        # recognize speech using Google Cloud Speech
        GOOGLE_CLOUD_SPEECH_CREDENTIALS = r"""{
              "type": "service_account",
              "project_id": "arcane-geode-244410",
              "private_key_id": "9ac9dc2c39d35f136751b330b41309cdead95b5a",
              "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC1n7hrvQ7ornJr\nSzZRTehRKc6xax9zdjz3F7BhNOJVLeaRyw19VZ5+1rTAO2Htea+Itoj5TSeXhO6a\ni0FY0aeEk/3ThNskyE4leXlUhZm+wVNk6muSHR+Nt8BzdeZRlv9lfJJ1XKy21Z3i\nALSo+1P7WWfDOTxeHr6CAMKxUITF9V3g0i+Jlh/5q7NRh0u4hBE9e29XTjPLAkHO\n1XZAZF0zdSrUaU5qjKZSaB0VdLW3/la8fqAR/8xhkuux/gqtMSxoNtQyA2XznsT4\n/Z0D6NLZ/KyMWYqUFcr83ud4voTJt5fTKY5bbl0SEptg6dWuqNW4o00rolZgn6Nm\nqBQMM8KDAgMBAAECggEAFefoRfJdGHpl6YP+TqsfYJ6yw9pgRjxWGRVd3DOVMuHg\n4+fCPkIz5id+UIs5AfwBzHL7rVn9cvyBDNnL7WsIEekJndfx2ItiRYnTtPqSMxw9\nMzhE9jAWDLFDgImRWHjmX/a48XfWiwzTyJz+LT13ASNk7Pp7yfxaTPLLiLhl8zwz\nWnrOqGCQh6zCpZOBoyOlk6rNmpVgVd6MWNKK7gD4qE0wEYplwo3ifWNR4AKH0SBq\n2SEmc7mTM0kK2N/Wx1Zf7IqqJFloCl2fndVmwNH2wJ7UXfCQQx8rSbtMTDcqNaAb\nArfYEh0iemkRZbOjPE2pDdlHeNrGrCMNlifI6hLp1QKBgQDjUxcKeKXa4JihxOQA\nX08YZ0Bb1HjRscRHhd7q0p1hB4jXp1QdsRNOurbkQw96xDV19l7ejr7BXhgdCb+g\nUi0Mdydkh36ZBmYLekGl1aBK17hjTLnRxAYbT+E40pqUO4jiEodNkOnMWBHEvuA7\nylHIan7yxy6OmvXYBFELQqlKDQKBgQDMiNXgIBJ0/XB2DOJHhaofpFv45JfZ6860\n2jAvRW1AsVxrUBwo7+UfllaUWhkxLsDMYpFRIpyNx3F5wiWmozNu4wBrRvoXmfNF\npkcYe7Zy+JSa96nBVzJXw/dqGi7OzJTN6ymBpYq3P74H8vRPGNNv+IRmwn61wHU5\nSAevUuXqzwKBgQC/zyJH4ZkAYDYpeTpw3fqdOrHhShxPw1E8kEaRmPjWIEdkv3mI\nPBtweLdNTxBGpDv/tsxBuexWuJBcIBeirPP56qhAeNtnPzDnSlcOGHzR6cdZcinw\nRxhn/zSof3uSy9EqADxORJIhq8YCXpRRntr24nUPYOV1ymbZyHqhp7O0GQKBgEMc\noxDndSbuAfi6QYU2VpwLUiJY33qh5TFyZl0carzPdYPAxXypOoUiz0XeSmXY8Woe\n7Y+xy6y5xIIvMit7YWUBFyIcJ/OWkDxKUwm1jigraJdwrELEWcByjLxD2xgACNOI\nVhY0O0/lmnUBQyiw/K/A27DcILeCbvRcrijZCpgBAoGAHlbnMNa0OhM0cBeDZmLM\n6zGJCSYbn/ZaToLfoy5P+l5QMDz0MGuOhnQ/UQKgFCegZsIKHFKVf6GFqQgw5+TW\nP8Gc6jboor1pMWzCYtL+tj1/0GKulFVavqoP/cE1I3vw97si94WLJqSnuNO74syJ\nSK5AMs8GnVtCW4Qrknvbwsk=\n-----END PRIVATE KEY-----\n",
              "client_email": "testing@arcane-geode-244410.iam.gserviceaccount.com",
              "client_id": "117003047473942590976",
              "auth_uri": "https://accounts.google.com/o/oauth2/auth",
              "token_uri": "https://oauth2.googleapis.com/token",
              "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
              "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/testing%40arcane-geode-244410.iam.gserviceaccount.com"
            }
            """
        try:
            translation = r.recognize_google_cloud(audio, credentials_json=GOOGLE_CLOUD_SPEECH_CREDENTIALS)
            
            # save it, or whatever
            
        except sr.UnknownValueError:
            print("Google Cloud Speech could not understand audio")
        except sr.RequestError as e:
            print("Could not request results from Google Cloud Speech service; {0}".format(e))
            
            