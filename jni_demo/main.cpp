/*************************************************************************
* COPYRIGHT NOTICE
*  Copyright (c) 2018, Wuhan Live Youxuan Online Education Co., Ltd.
*  All rights reserved.
*
*  @version : 1.0
*  @author : mxl
*  @E-mail : xiaolongicx@gmail.com
*  @date : 2018-11-30 14:58
*
*  Revision Notes :
*/

#include <iostream>
#include <jni.h>
#include <memory.h>

using namespace std;

int main()
{
    char opt1[] = "-Djava.compiler=NONE"; /** 暂时不知道啥意思，网上抄来的 */
    char opt2[] = "-Djava.class.path=.";  /** 指定Java类编译后.class文件所在的目录 */
    char opt3[] = "-verbose:NONE";        /** 暂时不知道啥意思，网上抄来的 */

    JavaVMOption options[3];

    options[0].optionString = opt1;
	options[0].extraInfo = NULL;

    options[1].optionString = opt2;
	options[1].extraInfo = NULL;

    options[2].optionString = opt3;
	options[2].extraInfo = NULL;

    JavaVMInitArgs jargv;
    jargv.version = JNI_VERSION_1_8; /** JDK JNI VERSION*/
    jargv.nOptions = 3;
    jargv.options = options;
    jargv.ignoreUnrecognized = JNI_TRUE;

    JavaVM* jvm = NULL;
    JNIEnv* jenv = NULL;

    jint res = JNI_CreateJavaVM( &jvm, (void**)&jenv, &jargv );
    if ( 0 != res )
	{
		cout << "create failed" << endl;

        return 1;
	}

    jclass jc = jenv->FindClass( "A" );
    if ( NULL == jc )
	{
		cout << "find failed" << endl;

        return 1;
	}

    jmethodID jmid = jenv->GetStaticMethodID( jc, "f", "()V" );
    if ( NULL == jmid )
	{
		cout << "get failed" << endl;

        return 1;
	}

    jenv->CallStaticVoidMethod( jc, jmid );

    std::cout << "Hello, JNI!" << std::endl;

    return 0;
}

