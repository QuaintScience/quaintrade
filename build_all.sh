#!/bin/bash
set -e

if [[ "$1" == "sudo" ]]
then
    sudo apt update
else
    apt update
fi

if [[ "$1" == "sudo" ]]
then
    sudo apt install -y software-properties-common gnupg2
else
    apt install -y software-properties-common gnupg2
fi


packages=("quaintrade")
for package in ${packages[@]}
do
    echo $package
    APT_KEYS_FILE=${package}/PACKAGE.apt.keys
    if [ -f ${APT_KEYS_FILE} ]
    then
        while read p; do
            if [[ "$1" == "sudo" ]]
            then
                sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys "$p"
            else
                apt-key adv --keyserver keyserver.ubuntu.com --recv-keys "$p"
            fi
        done <${APT_KEYS_FILE}
    fi
    APT_REPOS_FILE=${package}/PACKAGE.apt.repos
    if [ -f ${APT_REPOS_FILE} ]
    then
        while read p; do
            if [[ "$1" == "sudo" ]]
            then
                sudo add-apt-repository "$p"; sudo apt update
            else
                add-apt-repository "$p"; apt update
            fi
        done <${APT_REPOS_FILE}
    fi
    APT_INSTALL_FILE=${package}/PACKAGE.apt.installs
    if [ -f ${APT_INSTALL_FILE} ]
    then
        while read p; do
            if [[ "$1" == "sudo" ]]
            then
                sudo apt install -y $p
            else
                apt install -y $p
            fi
        done <${APT_INSTALL_FILE}
    fi
    ./pyb $package
    if [ $? -ne 0 ]; then
        echo "Pybuilder failed."
        exit 1
    fi
    CWD=$(pwd)
    if [ -z ${BITBUCKET_BUILD_NUMBER} ]
    then
        BITBUCKET_BUILD_NUMBER="dev"
    fi
    VERSION=$(cat ${package}/PACKAGE.version).${BITBUCKET_BUILD_NUMBER}
    SUBPACKAGE=$(cut -d'-' -f2 <<< "$package")
    TOP_LEVEL_INIT="${package}/src/main/python/quaintscience/${SUBPACKAGE}/__init__.py"
    sed -i '/version/d' ${TOP_LEVEL_INIT}
    echo "__version__ = \"${VERSION}\"" >> $TOP_LEVEL_INIT
    cd $package/target/dist/$package-$(cat ${package}/PACKAGE.version).${BITBUCKET_BUILD_NUMBER}
    rm -rf *egg-info dist build
    python setup.py sdist
    if [ $? -ne 0 ]; then
        echo "Compilation of source distribution failed."
        exit 1
    fi
    cd dist
    ls
    rm -f *linux*
    if [[ "$1" == "sudo" ]]
    then
	    sudo pip install *tar.gz
    else
	    pip install *tar.gz
        if [ $? -ne 0 ]; then
            echo "Pip install of compiled package failed."
            exit 1
        fi
    fi
    if [[ "$1" == "upload" ]]
    then
        twine upload --repository-url=${NEXUS_URL} -u ${NEXUS_USERNAME} -p ${NEXUS_PASSWORD} --non-interactive *tar.gz
        if [ $? -ne 0 ]; then
            echo "Twine failed."
            exit 1
        fi
    fi
    cd $CWD
done

if [[ "$1" == "upload" ]]
then
    pdoc -c show_source_code=False --html quaintrade/src/main/python/quaintscience --output-dir ../../docs
fi
