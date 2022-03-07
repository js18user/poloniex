""" Python v.3.8.7   js18user for  ap@agentapp.ru """


class PrError(Exception):
    pass


class ClientError(PrError):
    pass


def api_information(question):

    import requests

    answer = True

    try:

        response = requests.get(question)

        answer = response

        print(f'It is a information about status code: {response.status_code}')

    except ClientError as error:
        print('error : ', error)

    finally:
        pass

    return answer


def questions():

    skip = '\n'

    answer = api_information('https://api.github.com')
    print(f'It is a information into response: {skip}{answer.json()}')
    input('Please enter')

    answer = api_information('https://api.github.com/users')
    print(f'It is a information into response: {skip}{(answer.json())}')
    input('Please enter')

    answer = api_information('https://api.github.com/users/js18user')
    print(f'It is a information into response: {skip}{(answer.json())}')
    input('Please enter')

    answer = api_information('https://api.github.com/users/js18user/repos')
    print(f'It is a information into response: {skip}{(answer.json()[1])}')
    input('Please enter')

    print(f'It is a information into response: {skip}')
    [print(repository['html_url']) for repository in answer.json()]

    return()


def main():

    questions()

    return()


if __name__ == "__main__":

    main()

exit()
